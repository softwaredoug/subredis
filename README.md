subredis
========

[![Build Status](https://travis-ci.org/softwaredoug/subredis.svg?branch=master)](https://travis-ci.org/softwaredoug/subredis)

SubRedis is a simple namespace wrapper for Redis, generating completely sandboxed redises to enhance application safety and modularity.

Subredis allows creating redis's within an existing redis (or subredis) by prepending a prefix to the key before interacting with redis. Each subredis exists in a redis-compatible but isolated bubble unaffected by other subredis activity going on in the application. See [sample usage](##Sample Usage) for examples.

You can read more in [this blog post](http://www.opensourceconnections.com/?p=4800&preview=true).

## Installation

     pip install subredis


## Sample Usage

### Basic Usage

     from subredis import SubRedis

     redis = redis.from_url('redis://localhost:6379')
     subred = SubRedis("keyspace", redis)
     
     subred.set("foo", "bar")   # stores keyspace_foo -> bar
     subred.get("foo")          # gets keyspace_foo
     subred.flushdb()           # flushes everything in "keyspace"
     
### Create hierarchy within redis
 
     # Redis storing info about this student's classes
     subred = SubRedis(studentId + "-classes", redis)
     subred.set("name", "Biology")
     subred.set("teacher", "Professor Tim")
     
     # Redis storing info about this student's grades in this class
     subred2 = SubRedis(classId + "-grades", subred)
     subred2.set("Midterm", "89")
     subred2.set("Final Project", "72")
     
     
## Supported Features

Subredis intends to support nearly all features of a StrictRedis instance with the following exceptions:

1. Lua Scripting
2. Many Redis "Admin" interface calls (ie bgsave, etc)

## License, etc

Released under Apache license. See [here]([LICENSE.txt]) for more info.

(C) Doug Turnbull, 2013
