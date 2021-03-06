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
RESP_OK = object()

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
    hash_class = dict
    def hash_factory(self, value):
        return dict(value)


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

    def sendResponse(self, value):
        if value == NO_DATA:
            self.sendLine('$-1')
        elif isinstance(value, self.list_class):
            self._sendList(value)
        elif isinstance(value, self.string_class):
            self._sendString(value)
        elif isinstance(value, self.hash_class):
            self._sendHash(value)
        elif isinstance(value, int):
            self._sendInt(value)
        elif value == RESP_OK:
            self.sendLine('+OK')


    def _sendString(self, value):
        self.sendLine('$%d' % len(value))
        self.sendLine(value)

    def _sendList(self, value):
        lines = self.encodeValue(value)
        for line in lines:
            self.sendLine(line)

    def _sendInt(self, value):
        self.sendLine(':%d' % value)

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
                if isinstance(v, self.string_class):
                    lines.append('$%s' % len(v))
                    lines.append(v)
                else:
                    lines.append('$-1')
            return lines
        return [ str(value) ]

    def decodeValue(self, value):
        return value

    def _receiveInitial(self):
        while True:
            line = yield

            if line[0] == '*':
                count = self._readLength(line, prefix='*')
                if count is None:
                    return
                self.receiver = self._receiveMultiBulk(count)
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
            if not parsed:
                log.msg('_receiveInitial bad line: %s' % line)
                self.sendLine('-you messed up haus: %s' % parsed.expression)
                self.transport.loseConnection()
                return
            token = parsed.expression
            tokens = []
            while not isinstance(token, Stop):
                if token.collect:
                    tokens.append(token)
                old_parser = parser
                parser = parser.switch()
                if parser == old_parser:
                    break
                yield
                line = yield
                parsed = parser.parse(line)
                if not parsed:
                    self.sendLine('-you messed up haus: %s' % parsed.expression)
                    self.transport.loseConnection()
                    return
                token = parsed.expression
                if isinstance(token, Stop):
                    break
                yield
            for command in tokens:
                command.respond(self)
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
            bytes = yield
            bytes = self._readLength(bytes)
            if bytes is None:
                return
            data_size = int(bytes)
            #
            # He're well switch to a stream receiver to receiv N bytes and
            # call use back when we've read the data
            # 
            self._switchToStringReceiver(data_size, self._multiBulkDataReceived)
            yield
            data = yield
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
            if i == (count - 1):
                break
            yield
        self.receiver = self._receiveInitial()
        self._evaluateMultiBulk(tokens)

    def _switchToStringReceiver(self, data_size, callback):
        stringProtocol = BinaryStreamer(1)
        prefix = struct.pack('!I', data_size + 2)
        self.switchProtocol(stringProtocol, prefix, callback)
 
    def _multiBulkDataReceived(self, data):
        pred = lambda s: P(s, locals())
        string, newline = data[:-2], data[-2:]
        bad = pred("newline == '\r\n'")
        if bad:
            log.msg('_multiBuildDataReceived not a line ending: %s' % newline)
            return
        debug('-- Redis._multiBuildDataReceived len:%d' % len(string))
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
            command.respond(self)
             
    def _noop(self, *p, **kw):
        x = yield

   
class RedisFactory(Factory):

    protocol = Redis    

    def __init__(self):
        self.databases = [ {} for i in range(100) ]
        self.protocols = []
        self.popQueues = [ {} for i in range(100) ]

    def buildProtocol(self, addr):
        redis = Factory.buildProtocol(self, addr)
        redis.factory = self
        redis.delayedCalls = {}
        self.protocols.append(redis)
        return redis

    def listenPop(self, popMethod, databaseNo, protocol, keys, timeout):
        from twisted.internet import reactor
        entry = (protocol, popMethod, keys)
        queues = self.popQueues[databaseNo]
        for key in keys:
            dq = queues.setdefault(key, deque())
            dq.append(entry)
        reactor.callLater(timeout, self.unlistenPop, popMethod, databaseNo, protocol, keys, True)

    def unlistenPop(self, popMethod, databaseNo, protocol, keys, timeout=False):
        entry = (protocol, popMethod, keys)
        queues = self.popQueues[databaseNo]
        for key in keys:
            dq = queues.get(key, None)
            if dq and entry in dq:
                dq.remove(entry)
        if timeout:
            protocol.sendResponse(NO_DATA)

    def notifyPush(self, key, lst, databaseNo): 
        queues = self.popQueues[databaseNo]
        q = queues.get(key, None)
        if q:
            protocol, popMethod, keys = q.popleft()
            method = getattr(lst, popMethod)
            value = method()
            resp = protocol.list_factory([key, value])
            self.unlistenPop(popMethod, databaseNo, protocol, keys)
            protocol.sendResponse(resp)

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
            args = self.cb_args
            kwargs = self.cb_kwargs
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
        parts = line.split(' ')
        args = parts[1:]
        command = parts[0]
        if command.lower() not in COMMANDS:
            return Predicate(False, 'Invalid Command: `%s`' % command)
        self.command = globals()['Command%s' % command.upper()](self)
        self.command.setInlineArgs(args)
        return Predicate(True, self.command)


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


class VarArgsCommand(Command):

    argCount = 1024

    def setInlineArgs(self, args):
        if len(args):
            self.args = args
            self.commandParser.parser = self.commandParser
            self.parser = self.commandParser
        else:
            self.args = []
            self.commandParser.parser = self.parser = ValueParser(1, self.setNext)

    def setNext(self, args):
        self.args.append(args[0].value)
        self.argCount -= 1
        if self.argCount:
            pr = self.parser.parser = ValueParser(1, self.setNext)
            self.parser = pr
        else: 
            pr = self.parser.parser = self.commandParser
            self.parser = pr


class KeyCommand(VarArgsCommand):
    argCount = 1

    @property
    def key(self):
        return self.args[0]

class KeyKeyCommand(VarArgsCommand):
    argCount = 2

    @property
    def key1(self):
        return self.args[0]

    @property
    def key2(self):
        return self.args[1]


class KeyValueCommand(KeyCommand):
    argCount = 2
    secondArgBytes = True

    def setInlineArgs(self, args):
        if len(args) == 2 and self.secondArgBytes:
            self.args = [ args[0] ]
            self.argCount -= 1
            bytes = args[1]
            self.commandParser.parser = self.parser = ValueParser(1, self.setNext)
        elif len(args) == self.argCount:
            self.args = args
            self.argCount -= 2
            self.commandParser.parser = self.commandParser
            self.parser = self.commandParser
        elif len(args)  == 0:
            self.args = []
            self.commandParser.parser = self.parser = ValueParser(1, self.setNext)
        else:
            self.commandParser.parser = None
            log.msg('Dunno how to handle these arguments: %s' % args)

    @property
    def value(self):
        return self.args[1]


class KeyValueValueCommand(KeyValueCommand):
    argCount = 3
    secondArgBytes = False

    @property
    def value1(self):
        return self.args[1]

    @property
    def value2(self):
        return self.args[2]


class Expirational(object):

    def expire(self, key, seconds, redis):
        from twisted.internet import reactor
        if not redis.database.has_key(key):
            return 0#redis.sendResponse(0)
        if seconds == 0:
            timeouts = redis.delayedCalls.get(key, [])
            for timeout in timeouts:
                timeout.cancel()
            redis.database.pop(key, None)
            return 1#redis.sendResponse(1)
        if redis.delayedCalls.has_key(key):
            return 0#redis.sendResponse(0)
        def expire():
            debug('expiring key: %s' % key)
            redis.delayedCalls[self.key].remove(cl)
            if not redis.delayedCalls[self.key]:
                del redis.delayedCalls[self.key]
            redis.database.pop(key, None)
        cl = reactor.callLater(seconds, expire)
        redis.delayedCalls.setdefault(self.key, []).append(cl)
        return 1#redis.sendResponse(1)
        

class CommandSET(KeyValueCommand):

    def respond(self, redis):
        database = redis.database
        value = redis.decodeValue(self.value)
        database[self.key] = value
        redis.sendResponse(RESP_OK)


class CommandSETNX(KeyValueCommand):

    def respond(self, redis):
        v = redis.database.get(self.key, NO_DATA)
        if v != NO_DATA:
            return redis.sendResponse(0)
        redis.database[self.key] = self.value
        return redis.sendResponse(1)
        

class CommandSETEX(KeyValueValueCommand, Expirational):

    def respond(self, redis): 
        database = redis.database
        seconds = int(self.value2)
        value = redis.decodeValue(self.value1)
        key = self.key
        database[key] = value
        self.expire(key, seconds, redis)
        redis.sendResponse(RESP_OK)


class CommandMSET(VarArgsCommand):

    def respond(self, redis):
        for i in range(0, len(self.args), 2):
            key, value = self.args[i:i+2]
            redis.database[key] = redis.decodeValue(value)
        redis.sendResponse(RESP_OK)

class CommandMSETNX(VarArgsCommand):
    
    def respond(self, redis):
        update = {}
        for i in range(0, len(self.args), 2):
            key, value = self.args[i:i+2]
            if redis.database.has_key(key):
                return redis.sendResponse(0)
            update[key] = value
        redis.database.update(update)
        redis.sendResponse(1)

class CommandGET(KeyCommand):

    def respond(self, redis):
        value = redis.database.get(self.key, NO_DATA)
        if value is NO_DATA:
            redis.sendResponse(NO_DATA)
        elif isinstance(value, redis.string_class):
            redis.sendResponse(value)
        else:
            redis.sendLine('-ERR Operation against a key holding the wrong kind of value')
       
 
class CommandMGET(VarArgsCommand):

    def respond(self, redis):
        vlist = redis.list_factory([])
        for key in self.args:
            vlist.append(redis.database.get(key, NO_DATA))
        redis.sendResponse(vlist)


class CommandEXISTS(KeyCommand):

    def respond(self, redis):
        exists = redis.database.has_key(self.key)
        if exists:
            redis.sendResponse(1)
        else:
            redis.sendResponse(0)


class CommandDEL(KeyCommand):

    def respond(self, redis):
        print self, 'evaluating with', redis
        value = redis.database.pop(self.key, NO_DATA)
        if value == NO_DATA:
            redis.sendResponse(0)
        else:
            redis.sendResponse(1)
    
class CommandTYPE(KeyCommand):

    def respond(self, redis):
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

    def respond(self, redis):
        pt = re.compile(self.key)
        matches = [ k for k in redis.database.keys() if pt.match(k) ]
        data = ' '.join(matches)
        redis.sendResponse(data)


class CommandPING(Command):

    def setInlineArgs(self, args):
        if len(args) == 0:
            self.commandParser.parser = self.commandParser

    def respond(self, redis):
        redis.sendLine('+PONG')

class CommandRANDOMKEY(NoArgsCommand):

    def respond(self, redis):
        key = random.choice(redis.database.keys())
        self.sendResponse(key)

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

    def respond(self, redis):
        redis.database_no = self.database_no
        redis.sendResponse(RESP_OK)

    def __str__(self):
        return 'CommandSELECT(database_no=%s)' % self.database_no


class CommandGETSET(KeyValueCommand):

    def respond(self, redis):
        old_value = redis.database.get(self.key, NO_DATA)
        value = redis.decodeValue(self.value)
        redis.database[self.key] = value
        if old_value == NO_DATA:
            redis.sendResponse(NO_DATA)
        else:
            redis.sendResponse(old_value)
      
 
class CommandEXPIRE(KeyValueCommand, Expirational):

    secondArgBytes = False

    def respond(self, redis):
        key = self.key
        seconds = int(self.value)
        if self.expire(key, seconds, redis):
            redis.sendResponse(1)
        else:
            redis.sendResponse(0)


class CommandTTL(KeyCommand):

    def respond(self, redis):
        key = self.key
        if not redis.database.has_key(key):
            return redis.sendResponse(-1)
        calls = redis.delayedCalls.get(key, None)
        if calls is not None:
            exp = min( [ cl.getTime()  for cl in calls ] )
            ttl = int(math.ceil(cl.getTime() - time.time()))
            if ttl >= 0:
                redis.sendResponse(ttl)
            else:
                redis.sendResponse(-1)
        else:
            redis.sendResponse(-1)


class CommandRENAME(KeyKeyCommand):

    secondArgBytes = False

    def respond(self, redis):

        if self.key1 == self.key2:
            return redis.sendLine('-source and destination objects are the same')

        value = redis.database.pop(self.key1, NO_DATA)
        if value == NO_DATA:
            redis.sendLine('-no such key')
        else:
            value = redis.decodeValue(value)
            redis.database[self.key2] = value
            redis.sendResponse(RESP_OK) 


class CommandMOVE(KeyValueCommand):

    secondArgBytes = False

    def respond(self, redis):

        key = self.key
        database_no = int(self.value)

        value = redis.database.pop(key, NO_DATA)
        if value == NO_DATA:
            return redis.sendResponse(0)
        dest_db = redis.factory.databases[database_no]
        dest_db[key] = value
        redis.sendResponse(1)


class CommandRENAMENX(KeyKeyCommand):

    secondArgBytes = False

    def respond(self, redis):

        if self.key1 == self.key2:
            return redis.sendLine('-source and destination objects are the same')

        value = redis.database.pop(self.key1, NO_DATA)
        if value == NO_DATA:
            return redis.sendLine('-no such key')
        destvalue = redis.database.pop(self.key2, NO_DATA)
        if destvalue != NO_DATA:
            redis.database[self.key1] = value
            redis.database[self.key2] = destvalue
            return redis.sendResponse(0)
        value = redis.decodeValue(value)
        redis.database[self.key2] = value
        redis.sendResponse(RESP_OK) 


class CommandDBSIZE(NoArgsCommand):

    def respond(self, redis):
        redis.sendResponse(len(redis.database))


class CommandINCR(KeyCommand):
    
    def respond(self, redis):
        value = redis.database.get(self.key, '0')
        if not value.isdigit():
            return redis.sendLine('-ERR value is not an integer')
        redis.database[self.key] = value = str(int(value) + 1)
        redis.sendResponse(int(value))

class CommandINCRBY(KeyValueCommand):

    secondArgBytes = False
    
    def respond(self, redis):
        value = redis.database.get(self.key, '0')
        if not value.isdigit():
            redis.sendLine('-ERR value is not an integer')
            return
        redis.database[self.key] = value = str(int(value) + int(self.value))
        redis.sendResponse(int(value))

class CommandDECR(KeyCommand):
    
    def respond(self, redis):
        value = redis.database.get(self.key, '0')
        if not value.isdigit():
            return redis.sendLine('-ERR value is not an integer')
        redis.database[self.key] = value = str(int(value) - 1)
        redis.sendResponse(int(value))

class CommandDECRBY(KeyValueCommand):

    secondArgBytes = False
    
    def respond(self, redis):
        value = redis.database.get(self.key, '0')
        if not value.isdigit():
            redis.sendLine('-ERR value is not an integer')
            return
        redis.database[self.key] = value = str(int(value) - int(self.value))
        redis.sendResponse(int(value))

class CommandAPPEND(KeyValueCommand):

    def respond(self, redis):
        value = redis.database[self.key]
        if not isinstance(value, redis.string_class):
            return redis.sendLine('-ERR Operation against a key holding the wrong kind of value')
        value += self.value
        redis.sendResponse(len(value))


class CommandSUBSTR(KeyValueValueCommand):

    def respond(self, redis):
        start = int(self.value1)
        stop = int(self.value2)
        if stop > 0:
            stop = stop + 1
        s = redis.database.get(self.key, NO_DATA)
        if s == NO_DATA:
            return redis.sendResponse(NO_DATA)
        if not isinstance(s, redis.string_class):
            return redis.sendLine('-ERR Operation against a key holding the wrong kind of value')
        if stop < 0:
            stop = len(s) + stop + 1
        substr = s[start:stop]
        redis.sendResponse(substr)

class PushCommand(KeyValueCommand):
    
    pushMethod = None
    secondArgBytes = True

    def respond(self, redis):
        lst = redis.database.setdefault(self.key, redis.list_factory([]))
        if not isinstance(lst, redis.list_class):
            redis.sendLine('-ERR wrong type for key')
            return
        method = getattr(lst, self.pushMethod)
        method(self.value)
        redis.factory.notifyPush(self.key, lst, redis.database_no)
        redis.sendResponse(len(lst))
    

class CommandLPUSH(PushCommand):
    pushMethod = 'appendleft'

class CommandRPUSH(PushCommand):
    pushMethod = 'append'
   
class PopCommand(KeyCommand):
    
    popMethod = None

    def respond(self, redis):
        lst = redis.database.setdefault(self.key, redis.list_factory([]))
        if not isinstance(lst, redis.list_class):
            redis.sendLine('-ERR wrong type for key')
            return
        method = getattr(lst, self.popMethod)
        value = method()
        redis.sendResponse(value) 

 
class CommandLPOP(PopCommand):
    popMethod = 'popleft'

class CommandRPOP(PopCommand):
    popMethod = 'pop'


class BlockingPopCommand(VarArgsCommand):
    
    popMethod = None
    queuePrefix = ''

    def respond(self, redis):
        
        keys = self.args[:-1]
        timeout = int(self.args[-1])

        for key in keys:
            lst = redis.database.setdefault(key, redis.list_factory([]))
            if not isinstance(lst, redis.list_class):
                redis.sendLine('-ERR wrong type for key')
                return
            if len(lst):            
                method = getattr(lst, self.popMethod)
                value = method()
                return redis.sendResponse(redis.list_factory([key, value])) 

        redis.factory.listenPop(self.popMethod, redis.database_no, redis,  keys, timeout)

class CommandBLPOP(BlockingPopCommand):
    popMethod = 'popleft'


class CommandBRPOP(BlockingPopCommand):
    popMethod = 'pop'


class CommandFLUSHDB(NoArgsCommand):

    def respond(self, redis):
        redis.factory.databases[redis.database_no] = {}
        redis.sendResponse(RESP_OK)


class CommandLLEN(KeyCommand):

    def respond(self, redis):
        l = redis.database.get(self.key, NO_DATA)
        if l == NO_DATA:
            return redis.sendResponse(NO_DATA)
        redis.sendResponse(len(l))


class CommandLRANGE(KeyValueValueCommand):

    def respond(self, redis):
        dq = redis.database.get(self.key, NO_DATA)
        key = self.key
        start = int(self.value1)
        stop = int(self.value2)
        if stop > 0:
            stop = stop + 1
        if dq == NO_DATA:
            return redis.sendResponse(redis.list_factory([]))
        if stop < 0:
            stop = len(dq) + stop + 1
        values = redis.list_factory(list(dq)[start:stop])
        redis.sendResponse(values)

class CommandSAVE(NoArgsCommand):

    def respond(self, redis):
        redis.sendResponse(RESP_OK)

CommandBGSAVE = CommandSAVE


class CommandINFO(NoArgsCommand):

    def respond(self, redis):
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
    
