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


def keyProxy(cls, fn_name):
    '''Create a method called fn_name in the SubRedis instance.
       This method appends a prefix to the key passed in,
       and then calls down into the inner redis object'''

    def fn_proxied(self, key, *args, **kwds):
        fn = getattr(self.redis, fn_name)
        prefixedKey = self.appendKeys(key)
        return fn(prefixedKey, *args, **kwds)
    setattr(cls, fn_name, fn_proxied)
    return cls


def unsupportedOperation(cls, fn_name, message=''):
    '''Create a method called fn_name in the SubRedis instance.
       This method throws a NotSupportedError when called'''
    def fn_unsupported(*args, **kwds):
        raise NotSupportedError(fn_name, message)
    setattr(cls, fn_name, fn_unsupported)
    return cls


def directProxy(cls, fn_name):
    '''Create a method called fn_name in the SubRedis instance.
    This method is just a redirect into the inner redis object'''

    def fn_proxied(self, key, *args, **kwds):
        fn = getattr(self.redis, fn_name)
        return fn(*args, **kwds)
    setattr(cls, fn_name, fn_proxied)
    return cls


def subredis_wrapper():
    def decorator(cls):
        mapping = [
            ('hget', keyProxy),
            ('append', keyProxy),
            # bgrewriteaof not supported for subredis
            # bgsave not supported for subredis
            ('bitcount', keyProxy),
            # bitopt custom impl below
            ('blpop', keyProxy),
            ('brpop', keyProxy),
            # brpoplpush custom impl below
            ('client_kill', unsupportedOperation),
            ('client_list', unsupportedOperation),
            ('client_get', unsupportedOperation),
            # dbsize TODO
            ('debug_object', keyProxy),
            ('decr', keyProxy),
            ('delete', keyProxy),  # TODO only supports deleting single key
            ('echo', unsupportedOperation),
            ('eval', unsupportedOperation),
            ('evalsha', unsupportedOperation),
            ('exists', keyProxy),
            ('expire', keyProxy),
            ('expireat', keyProxy),
            # flushall TODO  is possible?
            # flushdb implemented below
            # from_url not supported for subredi
            ('from_url', unsupportedOperation),
            ('get', keyProxy),
            ('getbit', keyProxy),
            ('getrange', keyProxy),
            ('getset', keyProxy),
            ('hdel', keyProxy),
            ('hexists', keyProxy),
            ('hget', keyProxy),
            ('hgetall', keyProxy),
            ('hincrby', keyProxy),
            ('hincrbyfloat', keyProxy),
            ('hkeys', keyProxy),
            ('hlen', keyProxy),
            ('hmget', keyProxy),
            ('hmset', keyProxy),
            ('hset', keyProxy),
            ('hsetnx', keyProxy),
            ('hvals', keyProxy),
            ('incr', keyProxy),
            ('incrbyfloat', keyProxy),
            # info TODO -- maybe info that this is a subredis?
            # keys -- custom implementation below
            ('lastsave', unsupportedOperation),
            ('lindex', keyProxy),
            ('linsert', keyProxy),
            ('llen', keyProxy),
            ('lock', keyProxy),
            ('lpop', keyProxy),
            ('lpush', keyProxy),
            ('lpushx', keyProxy),
            ('lrange', keyProxy),
            ('lrem', keyProxy),
            ('lset', keyProxy),
            ('ltrim', keyProxy),
            ('mget', keyProxy),
            ('move', unsupportedOperation),
            # mset -- custom impl below
            # msetnx -- custom impl below
            # object -- custom impl below
            ('parse_response', unsupportedOperation),
            ('persist', keyProxy),
            ('pexpire', keyProxy),
            ('pexpireat', keyProxy),
            ('ping', directProxy),
            ('pttl', keyProxy),
            # publish TODO pubsub possible here?
            # pubsub  TODO pubsub possible here?
            ('randomkey', unsupportedOperation),
            # register_script UNSUPPORTED
            # rename custom impl below
            # renamenx custom impl below
            ('rpop', keyProxy),
            # rpoplpush custom impl below
            ('rpush', keyProxy),
            ('rpushx', keyProxy),
            ('sadd', keyProxy),
            ('scard', keyProxy),
            # scripting not supported
            # sdiff -- custom impl below
            # sdiffstore -- custom impl below
            ('set', keyProxy),
            # set_response_callback not supported
            ('setbit', keyProxy),
            ('setex', keyProxy),
            ('setnx', keyProxy),
            ('setrange', keyProxy),
            # shutdown not supported
            ('sinter', keyProxy),
            # sinterstore custom impl below
            ('sismember', keyProxy),
            # slaveof not supported
            ('smembers', keyProxy),
            # smove custom impl below
            # TODO sort -- last key needs to be implemented
            ('spop', keyProxy),
            ('srandmember', keyProxy),
            ('srem', keyProxy),
            ('substr', keyProxy),
            ('sunion', keyProxy),
            # sunionstore -- custom impl below
            ('time', directProxy),
            # TODO transaction
            ('ttl', keyProxy),
            ('type', keyProxy),
            # TODO unwatch
            # TODO watch
            ('zadd', keyProxy),
            ('zcard', keyProxy),
            ('zincrby', keyProxy),
            ('zinterstore', keyProxy),
            ('zrange', keyProxy),
            ('zrangebyscore', keyProxy),
            ('zrank', keyProxy),
            ('zrem', keyProxy),
            ('zremrangebyrank', keyProxy),
            ('zremrangebyscore', keyProxy),
            ('zrevrange', keyProxy),
            ('zrevrangebyscore', keyProxy),
            ('zrevrank', keyProxy),
            ('zscore', keyProxy),
        ]
        for fn_name, wrapper in mapping:
            wrapper(cls, fn_name)
        return cls
    return decorator


@subredis_wrapper()
class SubRedis(object):

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
        return SubPipeline(self.prefix, self.redis.pipeline())

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


class SubPipeline(SubRedis):

    def __init__(self, prefix, pipeline):
        super(SubPipeline, self).__init__(prefix, pipeline)
        self.pipeline = pipeline

    def execute(self):
        return self.pipeline.execute()
