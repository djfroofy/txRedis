"""
@file server.py
"""
import struct
import re
import random
import time
import math
from collections import deque

from twisted.protocols.basic import LineOnlyReceiver, Int32StringReceiver
from twisted.python import log
from twisted.python.failure import Failure
from twisted.internet.protocol import Factory


from txredis.commands import COMMANDS

NO_DATA = object()

DEBUG = True

debug = lambda msg: None
if DEBUG:
    debug = log.msg



class _SwitchableMixin(object):

    def switchProtocol(self, proto, prefix, callback, *cb_args, **cb_kwargs):
        recvd = self.recvd
        self.recvd = ''
        if prefix:
            recvd = prefix + (recvd or '')
        
        if callback:
            proto.master = self
            proto.payloadsReceived = callback
            proto.cb_args = cb_args
            proto.cb_kwargs = cb_kwargs

        proto.makeConnection(self.transport)
        if recvd:
            proto.dataReceived(recvd)


class Predicate(object):

    def __init__(self, true, expression):
        self.true = true
        self.expression = expression

    def __nonzero__(self):
        return self.true

def P(_s_, _ns_):
    _truth_ = eval(_s_, _ns_, _ns_)
    return Predicate(_truth_, _s_)


def _same(something):
    return something

class Redis(LineOnlyReceiver, _SwitchableMixin):
    
    receiver = None
    database_no = 0

    string_class = str
    def string_factory(self, value):
        return value
    list_class = deque
    def list_factory(self, value):
        return deque(value)

    def connectionMade(self):
        debug('connection from client: %s' % self.transport)
        self.receiver = self._receiveInitial()

    def lineReceived(self, line):
        debug('-=-=-=-= line received: %s' % line)
        try:
            print '>> resuming generator', self.receiver
            self.receiver.next()
            print '>> sending line', line
            self.receiver.send(line)
            print '>> ok sent line', line
        except StopIteration, se:
            self.receiver = self._receiveInitial()
        except Exception, e:
            f = Failure(e)
            log.err(f)
            self.sendLine('-sorry haus. bye now: %s' % e)
            self.transport.loseConnection()
            self.receiver = self._noop()

    @property
    def database(self):
        return self.factory.databases[self.database_no]


    def _get_recvd(self):
        return self._buffer

    def _set_recvd(self, value):
        self._buffer = value

    recvd = property(_get_recvd, _set_recvd)

    def encodeValue(self, value):
        if isinstance(value, self.string_class):
            return [ str(value) ]
        elif isinstance(value, self.list_class):
            lines = []
            lines.append('*%s' % len(value))
            for v in value:
                lines.append('$%s' % len(v))
                lines.append(v)
            print '@@@@ responding %r' % lines
            return lines
        return [ str(value) ]

    def decodeValue(self, value):
        return value

#        if isinstance(value, self.string_class):
#            return self.string_factory(value)
#        if isinstance(value, self.list_class):
#            return str(self.list_factory(value))
#        return self.string_factory(value)

    def _receiveInitial(self):
        while True:
            line = yield

            print 'Got a line', line

            if line[0] == '*':
                count = self._readLength(line, prefix='*')
                if count is None:
                    return
                self.receiver = self._receiveMultiBulk(count)
                print 'now we have a receiver', self.receiver
                debug('_receiveInitial switched receiver to %s' % self.receiver)
                continue

            ls = locals()
            pred = lambda s : P('line[0] == "%s"' % s, ls)

            bad = pred(':') or pred('$') or pred('+') or pred('-')
           
            # handle normal commands
 
            if bad:
                log.msg('_receiveInitial bad prefix: %s' % line)
                self.sendLine('-you messed up haus: %s' % bad.expression)
                self.transport.loseConnection()
                return

            parser = CommandParser()
            parsed = parser.parse(line)
            print 'parsed', parsed
            if not parsed:
                log.msg('_receiveInitial bad line: %s' % line)
                self.sendLine('-you messed up haus: %s' % parsed.expression)
                self.transport.loseConnection()
                return
            token = parsed.expression
            tokens = []
            while not isinstance(token, Stop):
                print '%%%% ok the token is', token
                if token.collect:
                    tokens.append(token)
                old_parser = parser
                parser = parser.switch()
                print '%%%% ? switched', parser
                if parser == old_parser:
                    break
                yield
                line = yield
                parsed = parser.parse(line)
                if not parsed:
                    log.msg('%%%% _receiveInitial bad line: %s' % line)
                    self.sendLine('-you messed up haus: %s' % parsed.expression)
                    self.transport.loseConnection()
                    return
                token = parsed.expression
                print '%%%% got a token', token
                if isinstance(token, Stop):
                    break
                yield
            print '%%%% here is my tokens', tokens
            for token in tokens:
                token.eval(self)
            yield
                

    def _readLength(self, bytes, prefix='$'):
   
        lcs = locals()
 
        pred = lambda s: P(s, lcs)



        bad = ( pred("not bytes") or
                pred("bytes[0] != '%s'" % prefix) or
                pred("not bytes[1:].isdigit()") )
        if bad:
            log.msg('_readLength failed %r. %s' % (bad.expression, bytes))
            self.sendLine('-you messed up haus. bye.'
                          'failed assertion for length chunk: %r' % bad.expression)
            self.transport.loseConnection()
            return
        return int(bytes[1:])
   
    def _receiveMultiBulk(self, count):
        parser = CommandParser()
        tokens = []
        for i in range(count):
            print 'waiting for bytes'
            bytes = yield
            print 'ok got bytes', bytes
            bytes = self._readLength(bytes)
            if bytes is None:
                return
            data_size = int(bytes)
            #
            # He're well switch to a stream receiver to receiv N bytes and
            # call use back when we've read the data
            # 
            self._switchToStringReceiver(data_size, self._multiBulkDataReceived)
            print 'waiting for data'
            yield
            data = yield
            print 'ok got data'
            print 'parsing data', data
            print 'parsing with', parser
            parsed = parser.parse(data)
            if not parsed:
                log.msg('parser %r failed: %s' % (parser, parsed.expression))
                self.transport.loseConnection()
                self.receiver = self._noop()
                return
            token = parsed.expression
            if token.collect:
                tokens.append(token)
            parser = parser.switch()
            print 'switch parser', parser
            if i == (count - 1):
                break
            yield
        self.receiver = self._receiveInitial()
        self._evaluateMultiBulk(tokens)

    def _switchToStringReceiver(self, data_size, callback):
        stringProtocol = BinaryStreamer(1)
        print 'making prefix'
        prefix = struct.pack('!I', data_size + 2)
        print 'made prefix', prefix
        self.switchProtocol(stringProtocol, prefix, callback)
 
    def _multiBulkDataReceived(self, data):
        pred = lambda s: P(s, locals())
        string, newline = data[:-2], data[-2:]
        bad = pred("newline == '\r\n'")
        if bad:
            log.msg('_multiBuildDataReceived not a line ending: %s' % newline)
            return
        debug('-- Redis._multiBuildDataReceived len:%d' % len(string))
        print 'got multi bulk chunk', string
        self.receiver.next()
        self.receiver.send(string)


    def _evaluateMultiBulk(self, tokens):
        C = Command

        while tokens:
            command = tokens.pop(0)
            lcs = locals()
            pred = lambda s: P(s, lcs)
            bad = pred("not isinstance(command, C)")
            if bad:
                log.msg('_evaluateMultiBulk unexpected token %s '
                        'in multibulk stream: %s' % (command, bad.expression))
                self.transport.loseConnection()
                self.receiver = self._noop()
                return
            debug('_evaluateMultiBulk evaluating command: %r' % command)
            command.eval(self)
#            for token in tokens:
#                if isinstance(token)
             
    def _noop(self, *p, **kw):
        x = yield

   
class RedisFactory(Factory):

    protocol = Redis
    
    def __init__(self):
        self.databases = [ {} for i in range(100) ]
        self.protocols = []

    def buildProtocol(self, addr):
        redis = Factory.buildProtocol(self, addr)
        redis.factory = self
        redis.delayedCalls = {}
        self.protocols.append(redis)
        return redis

    def stopFactory(self):
        for protocol in self.protocols:
            protocol.transport.loseConnection()

 
class BinaryStreamer(Int32StringReceiver):

    def __init__(self, payload_ct):
        self.payload_ct = payload_ct
        self.payloads = []

    def stringReceived(self, string):
        self.payload_ct -= 1
        self.payloads.append(string)
        debug('BinaryStreamer.stringReceiver received string %s' % string)
        debug('BinaryStreamer.stringReceiver remaining paylods: %s' % self.payloads)
        if (not self.payload_ct) and self.payloadsReceived:
            print 'binary streamer calling back', self, self.payloadsReceived
            args = self.cb_args
            kwargs = self.cb_kwargs
            print 'ok'
            self.payloadsReceived(self.payloads, *args, **kwargs)


class Parser(object):

    parser = None

    def __init__(self):
        pass

    def parse(self, line):
        pass

    def switch(self):
        print '------ switching to parser', self.parser
        return self.parser


class CommandParser(Parser):

    def __init__(self):
        self.originalParser = self
        self.parser = self

    def parse(self, line):
        print 'CommandParser.parse parsing line', line
        parts = line.split(' ')
        args = parts[1:]
        command = parts[0]
        print 'ok "%s" %s' % (command, args)
        if command.lower() not in COMMANDS:
            return Predicate(False, 'Invalid Command: `%s`' % command)
        self.command = globals()['Command%s' % command.upper()](self)
        print 'ok', self.command
        print 'setting inline args', args
        self.command.setInlineArgs(args)
        print 'ok'
        return Predicate(True, self.command)

#    def switch(self):
#        return self.command.parser


class Token(object):
    collect = False


class Evalable(object):
    pass

class Command(Evalable, Token):
    parser = None
    collect = True

    def __init__(self, commandParser):
        self.commandParser = commandParser

    def setInlineArgs(self, args):
        return
       

class NoArgsCommand(Command):

    def setInlineArgs(self, args):
        if len(args) == 0:
            self.commandParser.parser = self.commandParser
            self.parser = self.commandParser

class _KeySetterMixin(object):

    afterSetKey = None
 
    def setKey(self, keys):
        log.msg('(set key) got keys')
        self.key = keys[0].value
        if self.afterSetKey:
            return self.afterSetKey()
        pr = self.parser.parser = self.commandParser
        self.parser = pr

class KeyCommand(Command, _KeySetterMixin):
    key = None

    def setInlineArgs(self, args):
        if len(args) == 1:
            self.key = args[0]
            self.commandParser.parser = self.parser = self.commandParser
            #self.commandParser.parser
        elif len(args) == 0:
            self.commandParser.parser = self.parser = ValueParser(1, self.setKey)




class KeyValueCommand(KeyCommand):

    afterSetValue = None
    secondArgBytes = True
        
    def setInlineArgs(self, args):
        if len(args) == 2 and self.secondArgBytes:
            self.key = args[0]
            bytes = args[1]
            self.commandParser.parser = self.parser = ValueParser(1, self.setValue)
        elif len(args) == 2:
            self.key = args[0]
            self.value = args[1]
            self.commandParser.parser = self.commandParser
            self.parser = self.commandParser
        elif len(args)  == 0:
            self.commandParser.parser = self.parser = ValueParser(1, self.setKey)
        else:
            self.commandParser.parser = None
            log.msg('Dunno how to handle these arguments: %s' % args)

    def afterSetKey(self):
        pr = self.parser.parser = ValueParser(1, self.setValue)
        self.parser = pr

    def setValue(self, values):
        self.value = values[0].value
        if self.afterSetValue:
            return self.afterSetValue()
        pr = self.parser.parser = self.commandParser
        self.parser = pr

class KeyValueValueCommand(KeyValueCommand):
    afterSetValue1 = None
    afterSetValue2 = None
    secondArgBytes = False
        
    def setInlineArgs(self, args):
        if len(args) == 3:
            self.key = args[0]
            self.value1 = args[1]
            self.value2 = args[2]
        elif len(args)  == 0:
            self.commandParser.parser = self.parser = ValueParser(1, self.setKey)
        else:
            self.commandParser.parser = None
            log.msg('Dunno how to handle these arguments: %s' % args)

    def afterSetKey(self):
        pr = self.parser.parser = ValueParser(1, self.setValue1)
        self.parser = pr

    def setValue1(self, values):
        self.value1 = values[0].value
        if self.afterSetValue1:
            return self.afterSetValue1()
        pr = self.parser.parser = ValueParser(1, self.setValue2)
        self.parser = pr
    
    def setValue2(self, values):
        self.value2 = values[0].value
        if self.afterSetValue1:
            return self.afterSetValue2()
        pr = self.parser.parser = self.commandParser
        self.parser = pr

class KeyKeyCommand(KeyValueCommand):

    @property
    def key1(self):
        return self.key

    @property
    def key2(self):
        return self.value



#class CommandSET(Command):
#
#    key = None
#    value = None
#
#    def setInlineArgs(self, args):
#        if len(args) == 2:
#            self.key = args[0]
#            bytes = args[1]
#            self.commandParser.parser = self.parser = ValueParser(1, self.setValue)
#        if len(args) == 0:
#            self.commandParser.parser = self.parser = ValueParser(1, self.setKey)
#        else:
#            self.commandParser.parser = None
#            log.msg('Dunno how to handle these arguments: %s' % args)
#
#    def setKey(self, keys):
#        log.msg('(set key) got keys')
#        self.key = keys[0].value
#        pr = self.parser.parser = ValueParser(1, self.setValue)
#        self.parser = pr
#
#    def setValue(self, values):
#        self.value = values[0].value
#        print '(set value)', self.value
#        print 'command parser', self.commandParser
#        pr = self.parser.parser = self.commandParser
#        self.parser = pr
#
#    def eval(self, redis):
#        print self, 'evaluating', redis
#        database = redis.database
#        value = redis.encodeValue(self.value)
#        print 'setting %s=%s' % (self.key, value)
#        database[self.key] = value
#        print 'done'
#        redis.sendLine('+OK')
#    
#    def __str__(self):
#        return 'CommandSET(key=%s, value=%s)' % (self.key, self.value)

class CommandSET(KeyValueCommand):

    def eval(self, redis):
        print self, 'evaluating', redis
        database = redis.database
        value = redis.decodeValue(self.value)
        print 'setting %s=%s' % (self.key, value)
        database[self.key] = value
        print 'done'
        redis.sendLine('+OK')


class CommandGET(KeyCommand):

    def eval(self, redis):
        print '', self, 'evaluating with', redis
        value = redis.database.get(self.key, NO_DATA)
        if value is NO_DATA:
            redis.sendLine('$-1')
        else:
            lines = redis.encodeValue(value)
            if len(lines) == 1:
                redis.sendLine('$%s' % len(lines[0]))
            #redis.sendLine('$%s' % len(value))
            #redis.sendLine(value)
            for line in lines:
                redis.sendLine(line)
        

class CommandEXISTS(KeyCommand):

    def eval(self, redis):
        print '', self, 'evaluating with', redis
        exists = redis.database.has_key(self.key)
        if exists:
            redis.sendLine(':1')
        else:
            redis.sendLine(':0')

class CommandDEL(KeyCommand):

    def eval(self, redis):
        print self, 'evaluating with', redis
        value = redis.database.pop(self.key, NO_DATA)
        if value == NO_DATA:
            redis.sendLine(':0')
        else:
            redis.sendLine(':1')
    
class CommandTYPE(KeyCommand):

    def eval(self, redis):
        v = redis.database.get(self.key, NO_DATA)

        if v == NO_DATA:
            return redis.sendLine('+none')

        if isinstance(v, redis.string_class):
            redis.sendLine('+string')
        elif isinstance(v, redis.list_class):
            redis.sendLine('+list')
        else:
            redis.sendLine('+string')

class CommandKEYS(KeyCommand):

    def eval(self, redis):
        pt = re.compile(self.key)
        matches = [ k for k in redis.database.keys() if pt.match(k) ]
        data = ' '.join(matches)
        redis.sendLine('$%s' % len(data))
        redis.sendLine(data)


class CommandPING(Command):

    def setInlineArgs(self, args):
        if len(args) == 0:
            self.commandParser.parser = self.commandParser

    def eval(self, redis):
        redis.sendLine('+PONG')

class CommandRANDOMKEY(NoArgsCommand):

    def eval(self, redis):
        key = random.choice(redis.database.keys())
        redis.sendLine('$%s' % len(key))
        redis.sendLine(key)

class CommandSELECT(Command):

    database_no = 0

    def setInlineArgs(self, args):
        if len(args) == 1:
            self.database_no = int(args[0])
            self.commandParser.parser = self.commandParser
            self.parser = self.commandParser
        else:
            self.commandParser.parser = self.parser = ValueParser(1, self.setNo)

    def setNo(self, nos):
        self.database_no = int(nos[0].value)
        pr = self.parser.parser = self.commandParser
        self.parser = pr

    def eval(self, redis):
        print '', self, 'evaluating with', redis
        redis.database_no = self.database_no
        redis.sendLine('+OK')

    def __str__(self):
        return 'CommandSELECT(database_no=%s)' % self.database_no


class CommandGETSET(KeyValueCommand):

    def eval(self, redis):
        print self, 'evaluating with', redis
        old_value = redis.database.get(self.key, NO_DATA)
        value = redis.decodeValue(self.value)
        redis.database[self.key] = value
        if old_value == NO_DATA:
            redis.sendLine('$-1')
        else:
            redis.sendLine('$%s' % len(old_value))
            redis.sendLine(old_value)
       
class CommandEXPIRE(KeyValueCommand):

    secondArgBytes = False

    def eval(self, redis):
        from twisted.internet import reactor
        key = self.key
        if not redis.database.has_key(key):
            return redis.sendLine(':0')
        def expire():
            debug('expiring key: %s' % key)
            redis.delayedCalls[self.key].remove(cl)
            if not redis.delayedCalls[self.key]:
                del redis.delayedCalls[self.key]
            redis.database.pop(key, None)
        cl = reactor.callLater(int(self.value), expire)
        redis.delayedCalls.setdefault(self.key, []).append(cl)
        redis.sendLine(':1')
        

class CommandTTL(KeyCommand):

    def eval(self, redis):
        key = self.key
        if not redis.database.has_key(key):
            return redis.sendLine(':-1')
        calls = redis.delayedCalls.get(key, None)
        if calls is not None:
            exp = min( [ cl.getTime()  for cl in calls ] )
            ttl = math.ceil(cl.getTime() - time.time())
            if ttl >= 0:
                redis.sendLine(':%d' % ttl)
            else:
                redis.sendLine(':-1')
        else:
            redis.sendLine(':-1')


class CommandRENAME(KeyKeyCommand):

    secondArgBytes = False

    def eval(self, redis):

        if self.key1 == self.key2:
            return redis.sendLine('-source and destination objects are the same')

        value = redis.database.pop(self.key1, NO_DATA)
        if value == NO_DATA:
            redis.sendLine('-no such key')
        else:
            value = redis.decodeValue(value)
            redis.database[self.key2] = value
            redis.sendLine('+OK') 


class CommandMOVE(KeyValueCommand):

    secondArgBytes = False

    def eval(self, redis):

        key = self.key
        database_no = int(self.value)

        value = redis.database.pop(key, NO_DATA)
        if value == NO_DATA:
            return redis.sendLine(':0')
        dest_db = redis.factory.databases[database_no]
        dest_db[key] = value
        redis.sendLine(':1')


class CommandRENAMENX(KeyKeyCommand):

    secondArgBytes = False

    def eval(self, redis):

        if self.key1 == self.key2:
            return redis.sendLine('-source and destination objects are the same')

        value = redis.database.pop(self.key1, NO_DATA)
        if value == NO_DATA:
            return redis.sendLine('-no such key')
        destvalue = redis.database.pop(self.key2, NO_DATA)
        if destvalue != NO_DATA:
            redis.database[self.key1] = value
            redis.database[self.key2] = destvalue
            return redis.sendLine(':0')
        value = redis.decodeValue(value)
        redis.database[self.key2] = value
        redis.sendLine('+OK') 

class CommandDBSIZE(NoArgsCommand):

    def eval(self, redis):
        data = str(len(redis.database))
        redis.sendLine('$%d' % len(data))
        redis.sendLine(data)


#class IncrCommandMixin(object):
#    amount = 1
#    
#    def eval(self, redis):
#        value = redis.database.get(self.key, '0')
#        if not value.isdigit():
#            redis.sendLine('-sorry haus')
#            return
#        redis.database[self.key] = value = str(int(value) + int(self.amount))
#        redis.sendLine(':%s' % value)

class CommandINCR(KeyCommand):
    
    def eval(self, redis):
        value = redis.database.get(self.key, '0')
        if not value.isdigit():
            redis.sendLine('-sorry haus')
            return
        redis.database[self.key] = value = str(int(value) + 1)
        redis.sendLine(':%s' % value)

class CommandINCRBY(KeyValueCommand):

    secondArgBytes = False
    
    def eval(self, redis):
        value = redis.database.get(self.key, '0')
        if not value.isdigit():
            redis.sendLine('-sorry haus')
            return
        redis.database[self.key] = value = str(int(value) + int(self.value))
        redis.sendLine(':%s' % value)

class CommandAPPEND(KeyValueCommand):

    def eval(self, redis):
        value = redis.database[self.key]
        value += self.value
        redis.sendLine(':%d' % len(value))


class CommandSUBSTR(KeyValueValueCommand):

    def eval(self, redis):
        start = int(self.value1)
        stop = int(self.value2)
        s = redis.database.get(self.key, NO_DATA)
        if s == NO_DATA:
            return redis.sendLine('$-1')
#        start = len(s) % start
#        stop = len(s) % stop
        substr = s[start:stop]
        redis.sendLine('$%d' % len(substr))
        redis.sendLine(substr)

class PushCommand(KeyValueCommand):
    
    pushMethod = None
    secondArgBytes = True

    def eval(self, redis):
        lst = redis.database.setdefault(self.key, redis.list_factory([]))
        if not isinstance(lst, redis.list_class):
            redis.sendLine('-ERR wrong type for key')
            return
        method = getattr(lst, self.pushMethod)
        method(self.value)
        redis.sendLine('+OK')
    

class CommandLPUSH(PushCommand):
    pushMethod = 'appendleft'

class CommandRPUSH(PushCommand):
    pushMethod = 'append'
   
class PopCommand(KeyCommand):
    
    popMethod = None

    def eval(self, redis):
        lst = redis.database.setdefault(self.key, redis.list_factory([]))
        if not isinstance(lst, redis.list_class):
            redis.sendLine('-ERR wrong type for key')
            return
        method = getattr(lst, self.popMethod)
        value = method()
        lines = redis.encodeValue(value)
        if len(lines) == 1:
            redis.sendLine('$%d' % len(lines[0]))
        for line in lines:
            redis.sendLine(line)
 
class CommandLPOP(PopCommand):
    popMethod = 'popleft'

class CommandRPOP(PopCommand):
    popMethod = 'pop'

class CommandFLUSHDB(NoArgsCommand):

    def eval(self, redis):
        redis.factory.databases[redis.database_no] = {}
        redis.sendLine('+OK')

class CommandSAVE(NoArgsCommand):

    def eval(self, redis):
        redis.sendLine('+OK')

CommandBGSAVE = CommandSAVE


class CommandINFO(NoArgsCommand):

    def eval(self, redis):
        version_key = 'redis_version'
        version_value = '3000'
        info = (
            ('redis_version', 'txredis-0.1'),
            ('connected_clients', '5000'),
            ('conencted_slaves', '1000'),
            ('used_memory', '3187'),
            ('changes_since_last_save', '0')
        )
        kv = '\r\n'.join([ ('%s:%s' % (k, v)) for  (k, v) in info ])
        redis.sendLine('$%d' % len(kv))
        redis.sendLine(kv)

class ValueParser(Parser):

    def __init__(self, payload_ct, callback):
        self.payload_ct = payload_ct
        self.callback = callback
        self.values = []

    def parse(self, line):
        self.values.append(Value(line))
        self.payload_ct -= 1
        if not self.payload_ct:
            self.callback(self.values)
            return Predicate(True, Stop())
        return Predicate(True, self.values[-1])


class Value(Token, Evalable):

    def __init__(self, value):
        self.value = value

    def eval(self):
        return self.value


class Stop(Token, Evalable):
    pass

#class CommandSELECT(Command):


if __name__ == '__main__':
    from twisted.internet import reactor
    from twisted.python import log
    import sys
    log.startLogging(sys.stdout)
    port = 6379
    if sys.argv[1:]:
        port = int(sys.argv[1]) 
    reactor.listenTCP(port, RedisFactory())
    reactor.run()
    
