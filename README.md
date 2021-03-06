# HBaseCache

HBaseCache is a class inhereting from werkzeug.contrib.cache's BaseCache that is meant to be used
together with Flask-Cache. Default usage is pretty simple. In your Flask application, once you've
installed `hbase_cache`:

```python
from time import sleep

from flask import Flask, request
from flask.ext.cache import Cache

app = Flask(__name__)

config = {'CACHE_TYPE': 'hbase_cache.hbase',
          'CACHE_HBASE_HOST': '127.0.0.1',
          'CACHE_HBASE_PORT': 9090,
          'CACHE_HBASE_TABLE': 'test',
          'CACHE_HBASE_PREFIX': None,
          'CACHE_DEFAULT_TIMEOUT': 300} 

# register the cache instance and binds it on to your app 
app.cache = Cache(app, config=config)   

def make_key():
  """Make a key that includes GET parameters."""
  return request.full_path


@app.route("/")
@app.cache.cached(timeout=300, key_prefix=make_key)  # cache this view for 5 minutes
def cached_view():
    return "Hello"


@app.route("/test/<int:n>")
@app.cache.cached(timeout=300, key_prefix=make_key)  # cache this view for 5 minutes
def numbers(n):
    sleep(2)  # sleeping so you can really check if it's caching
    return n


if __name__ == "__main__":
    app.run(port=5000, debug=True, host='0.0.0.0')
```