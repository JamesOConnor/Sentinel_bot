import os

SCIHUB_USER = os.environ.get('SCIHUB_USER')
SCIHUB_PASSWORD = os.environ.get('SCIHUB_PASSWORD')

SCIHUB_SEARCH_URL = 'https://scihub.copernicus.eu/dhus/search?start=0&rows=100&q=footprint:' \
                    '"Intersects(%s, %s)" AND ' \
                    'platformname:"Sentinel-2" AND ' \
                    'ingestiondate:[%s TO %s] AND ' \
                    'cloudcoverpercentage:[0 TO %s]&format=json'

CONSUMER_KEY = os.environ.get('CONSUMER_KEY')
CONSUMER_SECRET = os.environ.get('CONSUMER_SECRET')
ACCESS_KEY = os.environ.get('ACCESS_KEY')
ACCESS_SECRET = os.environ.get('ACCESS_SECRET')
