from redisspy import RedisSpy
from subRedis import SubRedis


def test_wrap_method():
    """ Test that the prefix is appended to methods that
        take a key as the first argument"""
    redis = RedisSpy()
    subredis = SubRedis("prefix", redis)
    redis.addSpy("hget")
    subredis.hget("foo")
    assert redis.lastCall == "hget"
    assert redis.lastArgs[0] == "prefix_foo"
