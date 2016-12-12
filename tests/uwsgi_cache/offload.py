import logging
logging.basicConfig(level='DEBUG')

if __name__ == '__main__':
    import os
    os.execlp('uwsgi', 'uwsgi', '-w', 'tests.uwsgi_cache.offload', '--master', '--need-app', '--http', ':8000',
              '--cache2', 'name=mycache,bitmap=1,items=100000,blocksize=1024,keysize=256',
              '--mule=tests/uwsgi_cache/mule.py')

import sys
sys.path.insert(0, '.')
import time
from cachel import make_cache
from cachel import uwsgi, redis, offload


l1 = uwsgi.UWSGICache('mycache')
l2 = redis.RedisCache()

offload_cache = offload.make_offload_cache(l1, l2, offload=uwsgi.offloader(1))
offload_worker = uwsgi.offload_worker(offload_cache)

@offload_cache('calc:{}', 10, 300)
def long_calc(val):
    time.sleep(1)
    return 'val-{}-{}'.format(val, time.time())

def application(env, start_response):
    start_response('200 OK', [('Content-Type','text/html')])
    return [repr(long_calc(10))]
