from setuptools import setup

installReq = """
nose>=1.3.0
redis>=2.7.0"""

setup(
    name="subredis",
    version="0.2.0",
    description="A Redis within your Redis",
    license="Apache",
    author="Doug Turnbull",
    author_email="softwaredoug@gmail.com",
    py_modules=['subredis'],
    install_requires=installReq,
    keywords=["redis", "database"],
    classifiers=[
        "Programming Language :: Python :: 3.4",
        'Programming Language :: Python :: 2.7',
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Database",
        "Development Status :: 4 - Beta"
    ],
    long_description="""\
Subredis Redis Keyspace Management
--------------------------------

Do you ever want to give part of your application its own redis instance
to do with as it please?

This is exactly what subredis does. With a single redis backing instance,
subredis wraps that instance and provides a fairly complete redis
implementation,storing the data in keys prefixed by a specified prefix. IE

    sr = SubRedis("subspace", redis)

    # Work within the "subspace" keyspace
    sr.set("foo", "bar")
    sr.get("foo")
    sr.flushdb()

Most direct uses of redis data structures are supported. Lua scripting and
many of the administrative methods are not supported.

Project on Github:
https://github.com/softwaredoug/subredis
""")
