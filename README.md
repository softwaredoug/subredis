subredis
========

Give components of your application their very own redis database.

Subredis is a simple redis wrapper that prepends a prefix to the key before interacting with redis. Each subredis works entirely in its own keyspace. So each owner of a subredis can flushdb, view its own keys, and pretend it exists in a redis-compatible but isolated bubble.

## Installation

     pip install subredis


## Sample Usage

     from subredis import SubRedis

     redis = redis.from_url('redis://localhost:6379')
     subred = SubRedis("keyspace", redis)
     
     subred.set("foo", "bar")   # stores keyspace_foo -> bar
     subred.get("foo")          # gets keyspace_foo
     subred.flushdb()           # flushes everything in "keyspace"
     
     
## Supported Features

Subredis intends to support nearly all features of a StrictRedis instance with the following exceptions:

1. Lua Scripting
2. Many Redis "Admin" interface calls (ie bgsave, etc)

## License, etc

Released under Apache license. See [here]([LICENSE.txt]) for more info.

(C) Doug Turnbull, 2013
