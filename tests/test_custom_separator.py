from redisspy import RedisSpy
from subredis import SubRedis, NotSupportedError


def test_default_separator():
    """ Test that it is stil working with original 
	separator """
    redis = RedisSpy()
    subredis = SubRedis("prefix", redis)
    assert subredis.appendKeys("foo") == "prefix_foo"

def test_custom_separator():
    """ Test that it works with custom separator """
    redis = RedisSpy()
    subredis = SubRedis("prefix", redis, separator=":")
    assert subredis.appendKeys("foo") == "prefix:foo"
