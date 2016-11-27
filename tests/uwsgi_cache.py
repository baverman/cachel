import sys
'.' in sys.path or sys.path.insert(0, '.')


def get_app():
    from cachel import make_cache
    from cachel.uwsgi import UWSGICache

    cache = make_cache(UWSGICache('mycache'))

    @cache.objects('square:{}', ttl=5)
    def get_squares(vals):
        print 'Cache miss', vals
        return {r: '12345'*500 for r in vals}

    def application(env, start_response):
        start_response('200 OK', [('Content-Type','text/html')])
        return [repr(get_squares(range(100)))]

    return application


if __name__ == '__main__':
    import os
    os.execlp('uwsgi', 'uwsgi', '-w', 'tests.uwsgi_cache', '--master', '--need-app', '--http', ':8000',
              '--cache2', 'name=mycache,bitmap=1,items=100000,blocksize=1024,keysize=256')
else:
    application = get_app()
