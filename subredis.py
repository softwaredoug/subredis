
class NotSupportedError(NotImplementedError):
    """ We intentionally do not support this feature
        we may support it in the future, but it requires
        pretty big thinking """
    def __init__(self, operation, message):
        super(NotSupportedError, self).__init__()
        self.operation = operation
        self.message = message


def keyProxyMethodFactory(redisMethod):
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
    def unsupportedMethod(subred, *args, **kwargs):
        raise NotSupportedError(redisMethod, message)
    return unsupportedMethod


def unsupportedOperation(methodName, dct, message=""):
    dct[methodName] = unsupportedOperationMethodFactory(methodName, message)


def directProxyMethodFactory(redisMethod):
    def directProxyMethod(subred, *args, **kwargs):
        return subred.redis(*args, **kwargs)
    return directProxyMethod


def directProxy(methodName, dct):
    dct[methodName] = directProxyMethodFactory(methodName)


class SubRedisMeta(type):

    def __new__(cls, clsname, bases, dct):
        print "meta called"
        keyProxy("hget", dct)
        keyProxy("append", dct)
        # bgrewriteaof not supported for subredis
        # bgsave not supported for subredis
        keyProxy("bitcount", dct)
        # bitopt TODO
        keyProxy("blpop", dct)
        keyProxy("brpop", dct)
        # brpoplpush TODO
        unsupportedOperation("client_kill", dct)
        unsupportedOperation("client_list", dct)
        unsupportedOperation("client_get", dct)
        # dbsize TODO
        keyProxy("debug_object", dct)
        keyProxy("decr", dct)
        keyProxy("delete", dct) #TODO only supports deleting single key
        unsupportedOperation("echo", dct)
        unsupportedOperation("eval", dct)
        unsupportedOperation("evalsha", dct)
        keyProxy("exists", dct)
        keyProxy("expire", dct)
        keyProxy("expireat", dct)
        # flushall TODO  is possible?
        # flushdb implemented below
        # from_url not supported for subredi
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
        # lastsave() not supported in subredis
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
        # move not supported
        # mset TODO -- needs custom impl
        # msetnx TODO -- needs custom impl
        # object TODO -- needs custom impl
        # parse_response not supported
        keyProxy("persist", dct)
        keyProxy("pexpire", dct)
        keyProxy("pexpireat", dct)
        directProxy("ping", dct)
        directProxy("pipeline", dct)
        keyProxy("pttl", dct)
        # publish TODO pubsub possible here?
        # pubsub  TODO pubsub possible here?
        # randomkey not supported
        # register_script TODO
        # rename TODO needs custom impl
        # renamenx TODO needs custom impl
        keyProxy("rpop", dct)
        # rpoplpush TODO needs custom impl
        keyProxy("rpush", dct)
        keyProxy("rpushx", dct)
        keyProxy("sadd", dct)
        keyProxy("scard", dct)
        # scripting not supported
        # sdiff -- TODO
        # sdiffstore TODO
        keyProxy("set", dct)
        # set_response_callback not supported
        keyProxy("setbit", dct)
        keyProxy("setex", dct)
        keyProxy("setnx", dct)
        keyProxy("setrange", dct)
        # shutdown not supported
        keyProxy("sinter", dct)
        # TODO sinterstore
        keyProxy("sismember", dct)
        # slaveof not supported
        keyProxy("smembers", dct)
        # TODO smove
        # TODO sort -- last key needs to be implemented
        keyProxy("spop", dct)
        keyProxy("srandmember", dct)
        keyProxy("srem", dct)
        keyProxy("substr", dct)
        keyProxy("sunion", dct)
        # TODO sunionstore
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
        #TODO zunionstore
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
        for key in allKeys:
            self.redis.delete(key)

    def keys(self, pattern="*"):
        """ Only run pattern matching on my values """
        lenOfPrefix = len(self.appendKeys(""))
        return [key[lenOfPrefix:] for key in self.redis.keys(self.appendKeys(pattern))]

    def rename(srcKey, destKey):
        raise NotImplementedError()

    def renamenx(srcKey, destKey):
        raise NotImplementedError()

    def bitop(operation, dest, keys):
        raise NotImplementedError()

    def brpoplpush(src, dst, timeout=0):
        raise NotImplementedError()

    def dbsize():
        raise NotImplementedError()

    def mset(mapping):
        raise NotImplementedError()
    
    def msetnx(mapping):
        raise NotImplementedError()

    def object(infotype, key):
        raise NotImplementedError()
