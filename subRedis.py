from functools import partial
from types import MethodType

def proxyRedis(subred, bound_redis_method, prefix, key, *args, **kwargs):
    prefixedKey = subred.appendKeys(key)
    rVal = bound_redis_method(prefixedKey, *args, **kwargs)
    return rVal

def proxyRedisDirect(subred, bound_redis_method, *args, **kwargs):
    return bound_redis_method(*args, **kwargs)


class SubRedis(object):
    """  Wrap the commands of redis by
         taking the passed in key and prefixing
         it with a key """

    def appendKeys(self, key):
        prefixedKey = key
        if self.prefix:
            prefixedKey = self.prefix + "_" + key
        return prefixedKey

    def __init__(self, prefix, redis):
        self.prefix = prefix
        self.redis = redis
        # Methods listed in the order they appear
        # in StrictRedis docs
        self.addKeyspaceProxy(redis.append)
        return
        # bgrewriteaof not supported for subredis
        # bgsave not supported for subredis
        self.addKeyspaceProxy(redis.bitcount)
        # bitopt TODO
        self.addKeyspaceProxy(redis.blpop)
        self.addKeyspaceProxy(redis.brpop)
        # brpoplpush TODO
        # client_kill not supported for subredis
        # client_list not supported for subredis
        # config_get not supported for subredis
        # dbsize TODOu
        self.addKeyspaceProxy(redis.debug_object)
        self.addKeyspaceProxy(redis.decr)
        self.addKeyspaceProxy(redis.delete) #TODO only supports deleting single key
        # echo not supported for subredis
        # eval TODO is possible?
        # evalsha TODO is possible?
        self.addKeyspaceProxy(redis.exists)
        self.addKeyspaceProxy(redis.expire)
        self.addKeyspaceProxy(redis.expireat)
        # flushall TODO  is possible?
        # flushdb implemented below
        # from_url not supported for subredi
        self.addKeyspaceProxy(redis.get)
        self.addKeyspaceProxy(redis.getbit)
        self.addKeyspaceProxy(redis.getrange)
        self.addKeyspaceProxy(redis.getset)
        self.addKeyspaceProxy(redis.hdel)
        self.addKeyspaceProxy(redis.hexists)
        self.addKeyspaceProxy(redis.hget)
        self.addKeyspaceProxy(redis.hgetall)
        self.addKeyspaceProxy(redis.hincrby)
        self.addKeyspaceProxy(redis.hincrbyfloat)
        self.addKeyspaceProxy(redis.hkeys)
        self.addKeyspaceProxy(redis.hlen)
        self.addKeyspaceProxy(redis.hmget)
        self.addKeyspaceProxy(redis.hmset)
        self.addKeyspaceProxy(redis.hset)
        self.addKeyspaceProxy(redis.hsetnx)
        self.addKeyspaceProxy(redis.hvals)
        self.addKeyspaceProxy(redis.incr)
        self.addKeyspaceProxy(redis.incrbyfloat)
        # info TODO -- maybe info that this is a subredis?
        # keys -- custom implementation below
        # lastsave() not supported in subredis
        self.addKeyspaceProxy(redis.lindex)
        self.addKeyspaceProxy(redis.linsert)
        self.addKeyspaceProxy(redis.llen)
        self.addKeyspaceProxy(redis.lock)
        self.addKeyspaceProxy(redis.lpop)
        self.addKeyspaceProxy(redis.lpush)
        self.addKeyspaceProxy(redis.lpushx)
        self.addKeyspaceProxy(redis.lrange)
        self.addKeyspaceProxy(redis.lrem)
        self.addKeyspaceProxy(redis.lset)
        self.addKeyspaceProxy(redis.ltrim)
        self.addKeyspaceProxy(redis.mget)
        # move not supported
        # mset TODO -- needs custom impl
        # msetnx TODO -- needs custom impl
        # object TODO -- needs custom impl
        # parse_response not supported
        self.addKeyspaceProxy(redis.persist)
        self.addKeyspaceProxy(redis.pexpire)
        self.addKeyspaceProxy(redis.pexpireat)
        self.addDirectProxy(redis.ping)
        self.addDirectProxy(redis.pipeline)
        self.addKeyspaceProxy(redis.pttl)
        # publish TODO pubsub possible here?
        # pubsub  TODO pubsub possible here?
        # randomkey not supported
        # register_script TODO
        # rename TODO needs custom impl
        # renamenx TODO needs custom impl
        self.addKeyspaceProxy(redis.rpop)
        # rpoplpush TODO needs custom impl
        self.addKeyspaceProxy(redis.rpush)
        self.addKeyspaceProxy(redis.rpushx)
        self.addKeyspaceProxy(redis.sadd)
        self.addKeyspaceProxy(redis.scard)
        # scripting not supported
        # sdiff -- TODO
        # sdiffstore TODO
        self.addKeyspaceProxy(redis.set)
        # set_response_callback not supported
        self.addKeyspaceProxy(redis.setbit)
        self.addKeyspaceProxy(redis.setex)
        self.addKeyspaceProxy(redis.setnx)
        self.addKeyspaceProxy(redis.setrange)
        # shutdown not supported
        self.addKeyspaceProxy(redis.sinter)
        # TODO sinterstore
        self.addKeyspaceProxy(redis.sismember)
        # slaveof not supported
        self.addKeyspaceProxy(redis.smembers)
        # TODO smove
        # TODO sort -- last key needs to be implemented
        self.addKeyspaceProxy(redis.spop)
        self.addKeyspaceProxy(redis.srandmember)
        self.addKeyspaceProxy(redis.srem)
        self.addKeyspaceProxy(redis.substr)
        self.addKeyspaceProxy(redis.sunion)
        # TODO sunionstore
        self.addDirectProxy(redis.time)
        # TODO transaction
        self.addKeyspaceProxy(redis.ttl)
        self.addKeyspaceProxy(redis.type)
        # TODO unwatch
        # TODO watch
        self.addKeyspaceProxy(redis.zadd)
        self.addKeyspaceProxy(redis.zcard)
        self.addKeyspaceProxy(redis.zincrby)
        self.addKeyspaceProxy(redis.zinterstore)
        self.addKeyspaceProxy(redis.zrange)
        self.addKeyspaceProxy(redis.zrangebyscore)
        self.addKeyspaceProxy(redis.zrank)
        self.addKeyspaceProxy(redis.zrem)
        self.addKeyspaceProxy(redis.zremrangebyrank)
        self.addKeyspaceProxy(redis.zremrangebyscore)
        self.addKeyspaceProxy(redis.zrevrange)
        self.addKeyspaceProxy(redis.zrevrangebyscore)
        self.addKeyspaceProxy(redis.zrevrank)
        self.addKeyspaceProxy(redis.zscore)
        #TODO zunionstore




    def addDirectProxy(self, redisMethod):
        """ Proxies that route directly back to redis, ie
            pipeline """
        proxyMethod = MethodType(proxyRedisDirect, self)
        proxyPartial = partial(proxyMethod, redisMethod)
        setattr(self, redisMethod.__name__, proxyPartial)

    def addKeyspaceProxy(self, redisMethod):
        """ Proxies that only work within a keyspace defined
            by self.prefix """
        proxyMethod = MethodType(proxyRedis, self)
        proxyPartial = partial(proxyMethod, redisMethod, self.prefix)
        setattr(self, redisMethod.__name__, proxyPartial)

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







if __name__ == "__main__":
    #red = TestRedis()
    #sred = SubRedis("prefix", red)
    #sred.erase("foo", "bar")

    # Test with settingsDb
    from store import redis
    from settingsDb import SettingsDb
    from settingsDb import Keys
    from settingsDb import Defaults
    redisProxy0 = SubRedis("0", redis)
    redisProxy1 = SubRedis("1", redis)
    sdb0 = SettingsDb(redisProxy0, forceReset=True)
    sdb1 = SettingsDb(redisProxy1, forceReset=True)

    # Check for stuff stored after the prefix
    # corresponds to defaults
    assert(redis.get("0_" + Keys.solrUrl) == Defaults.solrUrl)

    # Modify qdb for our 0_ proxy, confirm it works
    assert(sdb0.solrUrl == Defaults.solrUrl)
    assert(sdb1.solrUrl == Defaults.solrUrl)
    assert(redisProxy0.get(Keys.solrUrl) == Defaults.solrUrl)
    assert(redisProxy1.get(Keys.solrUrl) == Defaults.solrUrl)
    assert(redis.get("1_" + Keys.solrUrl) == Defaults.solrUrl)
    
    sdb0.setSolrUrl("foo")
    assert(redis.get("0_" + Keys.solrUrl) == "foo")
    sdb0.setSolrUrl(Defaults.solrUrl)

    print repr(redis.keys("0_*"))
    print repr(redis.keys("1_*"))

    # Test Keys
    sr = SubRedis("test1", redis)
    sr2 = SubRedis("test2", redis)
    sr.set("key1", "value1")
    sr2.set("key2", "value2")
    sr2.set("key3", "value3")
    keys1 = sr.keys()
    assert len(keys1) == 1
    print "KEYS: %s" % keys1[0]
    assert sr.get(keys1[0]) == "value1"
    keys2 = sr2.keys()
    assert len(keys2) == 2
