
class NotSupportedError(NotImplementedError):
    """ We intentionally do not support this feature
        we may support it in the future, but it requires
        pretty big thinking """
    def __init__(self, operation, message):
        super(NotSupportedError, self).__init__()
        self.operation = operation
        self.message = message

    def __str__(self):
        return "Unsupported Redis Operation: " + self.operation + \
               "performed (" + self.message + ")"


def keyProxyMethodFactory(redisMethod):
    """Create a method for the subredis object where
       we append a prefix to a key before calling redis"""
    def keyProxyMethod(subred, key, *args, **kwargs):
        """ Proxy redis for methods where the first arg is
            a redis key """
        prefixedKey = subred.appendKeys(key)
        proxiedRedisMethod = getattr(subred.redis, redisMethod)
        rVal = proxiedRedisMethod(prefixedKey, *args, **kwargs)
        return rVal
    return keyProxyMethod


def keyProxy(methodName, dct):
    dct[methodName] = keyProxyMethodFactory(methodName)


def unsupportedOperationMethodFactory(redisMethod, message):
    """Create a method for the subredis object where
       we thorw NotSupportedError on invocation"""
    def unsupportedMethod(subred, *args, **kwargs):
        raise NotSupportedError(redisMethod, message)
    return unsupportedMethod


def unsupportedOperation(methodName, dct, message=""):
    dct[methodName] = unsupportedOperationMethodFactory(methodName, message)


def directProxyMethodFactory(redisMethod):
    """Create a method for the subredis object where
       we just call redis normally, passing through everything"""
    def directProxyMethod(subred, *args, **kwargs):
        proxiedRedisMethod = getattr(subred.redis, redisMethod)
        return proxiedRedisMethod(args, **kwargs)
    return directProxyMethod


def directProxy(methodName, dct):
    dct[methodName] = directProxyMethodFactory(methodName)


class SubRedisMeta(type):

    def __new__(cls, clsname, bases, dct):
        keyProxy("hget", dct)
        keyProxy("append", dct)
        # bgrewriteaof not supported for subredis
        # bgsave not supported for subredis
        keyProxy("bitcount", dct)
        # bitopt custom impl below
        keyProxy("blpop", dct)
        keyProxy("brpop", dct)
        # brpoplpush custom impl below
        unsupportedOperation("client_kill", dct)
        unsupportedOperation("client_list", dct)
        unsupportedOperation("client_get", dct)
        # dbsize TODO
        keyProxy("debug_object", dct)
        keyProxy("decr", dct)
        keyProxy("delete", dct)  # TODO only supports deleting single key
        unsupportedOperation("echo", dct)
        unsupportedOperation("eval", dct)
        unsupportedOperation("evalsha", dct)
        keyProxy("exists", dct)
        keyProxy("expire", dct)
        keyProxy("expireat", dct)
        # flushall TODO  is possible?
        # flushdb implemented below
        # from_url not supported for subredi
        unsupportedOperation("from_url", dct)
        keyProxy("get", dct)
        keyProxy("getbit", dct)
        keyProxy("getrange", dct)
        keyProxy("getset", dct)
        keyProxy("hdel", dct)
        keyProxy("hexists", dct)
        keyProxy("hget", dct)
        keyProxy("hgetall", dct)
        keyProxy("hincrby", dct)
        keyProxy("hincrbyfloat", dct)
        keyProxy("hkeys", dct)
        keyProxy("hlen", dct)
        keyProxy("hmget", dct)
        keyProxy("hmset", dct)
        keyProxy("hset", dct)
        keyProxy("hsetnx", dct)
        keyProxy("hvals", dct)
        keyProxy("incr", dct)
        keyProxy("incrbyfloat", dct)
        # info TODO -- maybe info that this is a subredis?
        # keys -- custom implementation below
        unsupportedOperation("lastsave", dct)
        keyProxy("lindex", dct)
        keyProxy("linsert", dct)
        keyProxy("llen", dct)
        keyProxy("lock", dct)
        keyProxy("lpop", dct)
        keyProxy("lpush", dct)
        keyProxy("lpushx", dct)
        keyProxy("lrange", dct)
        keyProxy("lrem", dct)
        keyProxy("lset", dct)
        keyProxy("ltrim", dct)
        keyProxy("mget", dct)
        unsupportedOperation("move", dct)
        # mset -- custom impl below
        # msetnx -- custom impl below
        # object -- custom impl below
        unsupportedOperation("parse_response", dct)
        keyProxy("persist", dct)
        keyProxy("pexpire", dct)
        keyProxy("pexpireat", dct)
        directProxy("ping", dct)
        directProxy("pipeline", dct)
        keyProxy("pttl", dct)
        # publish TODO pubsub possible here?
        # pubsub  TODO pubsub possible here?
        unsupportedOperation("randomkey", dct)
        # register_script UNSUPPORTED
        # rename custom impl below
        # renamenx custom impl below
        keyProxy("rpop", dct)
        # rpoplpush custom impl below
        keyProxy("rpush", dct)
        keyProxy("rpushx", dct)
        keyProxy("sadd", dct)
        keyProxy("scard", dct)
        # scripting not supported
        # sdiff -- custom impl below
        # sdiffstore -- custom impl below
        keyProxy("set", dct)
        # set_response_callback not supported
        keyProxy("setbit", dct)
        keyProxy("setex", dct)
        keyProxy("setnx", dct)
        keyProxy("setrange", dct)
        # shutdown not supported
        keyProxy("sinter", dct)
        # sinterstore custom impl below
        keyProxy("sismember", dct)
        # slaveof not supported
        keyProxy("smembers", dct)
        # smove custom impl below
        # TODO sort -- last key needs to be implemented
        keyProxy("spop", dct)
        keyProxy("srandmember", dct)
        keyProxy("srem", dct)
        keyProxy("substr", dct)
        keyProxy("sunion", dct)
        # sunionstore -- custom impl below
        directProxy("time", dct)
        # TODO transaction
        keyProxy("ttl", dct)
        keyProxy("type", dct)
        # TODO unwatch
        # TODO watch
        keyProxy("zadd", dct)
        keyProxy("zcard", dct)
        keyProxy("zincrby", dct)
        keyProxy("zinterstore", dct)
        keyProxy("zrange", dct)
        keyProxy("zrangebyscore", dct)
        keyProxy("zrank", dct)
        keyProxy("zrem", dct)
        keyProxy("zremrangebyrank", dct)
        keyProxy("zremrangebyscore", dct)
        keyProxy("zrevrange", dct)
        keyProxy("zrevrangebyscore", dct)
        keyProxy("zrevrank", dct)
        keyProxy("zscore", dct)
        #zunionstore -- custom impl below
        return type.__new__(cls, clsname, bases, dct)


class SubRedis(object):
    __metaclass__ = SubRedisMeta

    def __init__(self, prefix, redis):
        self.redis = redis
        self.prefix = prefix

    def appendKeys(self, key):
        prefixedKey = key
        if self.prefix:
            prefixedKey = self.prefix + "_" + key
        return prefixedKey

    def flushdb(self):
        """ Should only flush stuff beginning with prefix?"""
        allKeys = self.redis.keys(self.appendKeys("*"))
        # for some reason deleteing with a list of keys isn't working
        p = self.redis.pipeline()
        for key in allKeys:
            p.delete(key)
        p.execute()

    def keys(self, pattern="*"):
        """ Only run pattern matching on my values """
        lenOfPrefix = len(self.appendKeys(""))
        return [key[lenOfPrefix:] for key in
                self.redis.keys(self.appendKeys(pattern))]

    def bitop(self, operation, dest, keys):
        keys = [self.appendKeys(key) for key in keys]
        return self.redis.bitop(operation, dest, keys)

    def brpoplpush(self, src, dest, timeout=0):
        src = self.appendKeys(src)
        dest = self.appendKeys(dest)
        return self.redis.brpoplpush(src, dest, timeout)

    def dbsize(self):
        raise NotImplementedError()

    def mset(self, mapping):
        mapping = {self.appendKeys(key): value for key, value in mapping}
        return self.redis.mset(mapping)

    def msetnx(self, mapping):
        mapping = {self.appendKeys(key): value for key, value in mapping}
        return self.redis.msetnx(mapping)

    def object(self, infotype, key):
        return self.redis.object(infotype, self.appendKeys(key))

    def pipeline(self, transaction=True):
        return SubPipeline(self.prefix, self.redis.pipeline)

    def rename(self, srcKey, destKey):
        srcKey = self.appendKeys(srcKey)
        destKey = self.appendKeys(destKey)
        return self.redis.rename(srcKey, destKey)

    def renamenx(self, srcKey, destKey):
        srcKey = self.appendKeys(srcKey)
        destKey = self.appendKeys(destKey)
        return self.redis.renamenx(srcKey, destKey)

    def rpoplpush(self, srcKey, destKey):
        srcKey = self.appendKeys(srcKey)
        destKey = self.appendKeys(destKey)
        return self.redis.rpoplpush(srcKey, destKey)

    def sdiff(self, keys, *args):
        keys = [self.appendKeys(key) for key in keys]
        return self.redis.sdiff(keys, *args)

    def sdiffstore(self, dest, keys, *args):
        dest = self.appendKeys(dest)
        keys = [self.appendKeys(key) for key in keys]
        return self.redis.sdiffstore(dest, keys, *args)

    def sinterstore(self, dest, keys, *args):
        dest = self.appendKeys(dest)
        keys = [self.appendKeys(key) for key in keys]
        return self.redis.sinterstore(dest, keys, *args)

    def smove(self, srcKey, destKey, value):
        srcKey = self.appendKeys(srcKey)
        destKey = self.appendKeys(destKey)
        return self.redis.smove(srcKey, destKey, value)

    def sort(name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None):
        raise NotImplementedError()

    def sunionstore(self, dest, keys, *args):
        dest = self.appendKeys(dest)
        keys = [self.appendKeys(key) for key in keys]
        return self.redis.sunionstore(dest, keys, *args)

    def transaction(func, watches, **kwargs):
        raise NotImplementedError()

    def watch(self, names):
        raise NotImplementedError()

    def unwatch(self):
        raise NotImplementedError()

    def zunionstore(self, dest, keys, aggregate=None):
        dest = self.appendKeys(dest)
        keys = [self.appendKeys(key) for key in keys]
        return self.redis.zunionstore(dest, keys, aggregate)


def SubPipeline(SubRedis):
    def __init__(self, prefix, pipeline):
        super(SubPipeline, self).__init__(prefix, pipeline)
        self.pipeline = pipeline

    def execute(self):
        return self.pipeline.execute()
