cachel
======

.. image:: https://travis-ci.org/baverman/cachel.svg?branch=master
   :target: https://travis-ci.org/baverman/cachel

.. image:: https://img.shields.io/badge/coverage-100%25-brightgreen.svg

.. image:: https://img.shields.io/badge/python-2.7%2C_3.4%2C_3.5%2C_3.6%2C_3.7%2C_pypy-blue.svg

Fast caches for python.

Features:

* Sync and async caches (``cachel.base.BaseCache`` and ``cachel.base.AsyncBaseCache``).
* Cache decorator supports sync and async (for python >= 3.5) functions.
* Batched requests via ``.objects`` decorator method.
* Explicit cache keys.
* Custom ttl for returned values (``cachel.expire``).
* Configurable serializers: (none, unicode, json/ujson, msgpack, pickle).
