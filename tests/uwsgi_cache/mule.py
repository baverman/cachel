import sys
sys.path.insert(0, '.')
from tests.uwsgi_cache.offload import offload_worker

if __name__ == '__main__':
    offload_worker()
