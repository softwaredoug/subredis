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
    result = [x.decode('utf-8') for x in result]
    assert result == ["bar", "snaz"]
    subredis.flushdb()


def test_keys():
    subredis = SubRedis("prefix", r)
    r.set("unrelated", "unrelated")
    subredis.set("foo", "bar")
    subredis.set("baz", "bar")
    subredis.set("cats", "bar")
    keys = subredis.keys()
    keys = [x.decode('utf-8') for x in keys]
    assert "foo" in keys
    assert "baz" in keys
    assert "cats" in keys
    print (repr(keys))
    assert len(keys) == 3


def test_set_get():
    subredis = SubRedis("prefix", r)
    r.set("unrelated", "unrelated")
    subredis.set("foo", "bar")
    subredis["baz"] = "snaz"
    assert subredis["foo"] == b"bar"
    assert subredis.get("foo") == b"bar"
    assert subredis["baz"] == b"snaz"
    assert subredis.get("baz") == b"snaz"
