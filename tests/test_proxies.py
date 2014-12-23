from redisspy import RedisSpy
from subredis import SubRedis, NotSupportedError


def test_prefix_wrap_method():
    """ Test that the prefix is appended to methods that
        take a key as the first argument"""
    redis = RedisSpy()
    subredis = SubRedis("prefix", redis)
    redis.addSpy("hget")
    subredis.hget("foo")
    assert redis.lastCall == "hget"
    assert redis.lastArgs[0] == "prefix_foo"


def test_unsupported_method():
    redis = RedisSpy()
    subredis = SubRedis("prefix", redis)
    try:
        subredis.client_kill()
        assert False
    except NotSupportedError:
        pass


def test_direct_proxy():
    redis = RedisSpy()
    subredis = SubRedis("prefix", redis)
    subredis.time("blah", cats="bananas")
    assert redis.lastCall == "time"
    assert redis.lastArgs[0][0] == "blah"
    assert redis.lastKwargs == {"cats": "bananas"}
