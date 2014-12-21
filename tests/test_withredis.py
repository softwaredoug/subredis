from subredis import SubRedis
import redis
r = redis.from_url('redis://localhost:6379')


def test_flushdb():
    subredis = SubRedis("prefix", r)
    r.set("unrelated", "unrelated")
    subredis.set("foo", "bar")
    subredis.set("baz", "bar")
    subredis.set("cats", "bar")
    subredis.flushdb()
    keys = subredis.keys()
    assert len(keys) == 0


def test_get_from_pipeline():
    subredis = SubRedis("prefix", r)
    subredis.set("foo", "bar")
    subredis.set("shaz", "snaz")
    pipeline = subredis.pipeline()
    pipeline.get("foo")
    pipeline.get("shaz")
    result = pipeline.execute()
    assert result == ["bar", "snaz"]
    subredis.flushdb()


def test_keys():
    subredis = SubRedis("prefix", r)
    r.set("unrelated", "unrelated")
    subredis.set("foo", "bar")
    subredis.set("baz", "bar")
    subredis.set("cats", "bar")
    keys = subredis.keys()
    assert "foo" in keys
    assert "baz" in keys
    assert "cats" in keys
    print (repr(keys))
    assert len(keys) == 3
