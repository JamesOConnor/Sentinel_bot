#!/usr/bin/env python
# encoding: utf-8

import calendar
import datetime
import os
import shutil
import time
import zipfile
from datetime import timedelta
from io import BytesIO
from random import randrange

import glob2
import numpy as np
import numpy.ma as ma
import rasterio as rio
import requests
import tweepy
from PIL import Image
from geopy.geocoders import Nominatim

import settings

screen_name = 'sentinel_bot'


def run_bot():
    """
    Starts the bot running in an infinite loop, posting an image every hour
    """
    bot = True
    start_time = datetime.datetime(2018, 1, 1)
    auth = (settings.SCIHUB_USER, settings.SCIHUB_PASSWORD)
    while bot is True:
        country, lat, lon = get_coordinates_and_country()
        rd = get_random_date()

        url = settings.SCIHUB_SEARCH_URL % (
            lat, lon, rd.strftime("%Y-%m-%dT%H:%M:%SZ"), (rd + timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ"), 0.1)

        res = requests.get(url, auth=auth)
        if res.status_code == 503:
            time.sleep(3600)
            continue
        if not res.ok:
            continue
        res_json = res.json()
        if int(res_json['feed']['opensearch:totalResults']) == 0:
            continue

        print(
            'Scihub resp: %s, Results count: %s' % (res.status_code, res_json['feed']['opensearch:totalResults'] or 0))

        im_date, preview, product_link = get_image_from_search(res_json)
        print('Image date: %s' % im_date)
        response = requests.get(preview, auth=auth)
        print('Preview status: %s' % response.status_code)

        if not response.ok:
            continue

        img = np.array(
            Image.open(BytesIO(response.content)))  # Read the preview and check if there is enough valid data
        print('Preview size:%s, %s, %s' % img.shape)
        if len(np.where(np.mean(img, axis=(2)) != 0)[0]) < img[:, :, 0].size * 0.9:
            continue

        print('Image found for %s' % country)
        download_tile(auth, product_link)
        try:
            unzip_tile()
        except zipfile.BadZipFile:
            continue
        bands, boa, false_colour, fp = retrieve_bands_for_image()

        image = read_bands(bands, fp)
        im = colour_balance_image(image)
        im.save('image.jpg')

        tweet_current_image(boa, country, false_colour, im_date, lat, lon, start_time)
        clean_up_directory()
        start_time = datetime.datetime.now()


def random_date(start, end):
    '''
    Returns a random date between two dates within our search timeframe, and a date a month after it
    :param start: Start date
    :param end: End date
    :return: A date between these dates, and a date a month after that
    '''
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


def get_country_osm(lat, lon):
    '''
    Returns the country parameter from a queried lat, lon using OSM's API - useful backup for when limits are exceeded/down
    :param lat: Latitiude to query
    :param lon: Longitude to query
    :return: Country of lat,lon according to OSMs' geolocater
    '''
    osm_geolocater = Nominatim(user_agent='sentinel_bot')
    loc = osm_geolocater.reverse((lat, lon), language='en')
    if 'error' in loc.raw.keys():
        return
    if 'country' not in loc.raw['address'].keys():
        return
    country = str(loc.raw['address']['country'])
    if 'water' in country:
        return None
    country = translate_country_names(country)
    return country


def translate_country_names(country):
    """
    Some of the country names retrieved from OSM need shortening/translation
    :param country: country name
    :return: updated country name
    """
    if country == 'Mexicanos':
        country = 'Mexico'
    elif country == 'PRC':
        country = 'China'
    elif country == 'Soomaaliya':
        country = 'Somalia'
    elif country == 'Viti':
        country = 'Fiji'
    elif country == 'RSA':
        country = 'South Africa'
    elif country == 'Brasil':
        country = 'Brazil'
    elif country == 'Naoero':
        country = 'Nauru'
    elif country == 'Madagasikara':
        country = 'Madagascar'
    elif country == 'Russian Federation':
        country = 'Russia'
    elif country == 'Norge':
        country = 'Norway'
    elif country == 'Cameroun':
        country = 'Cameroon'
    elif country == 'Deutschland':
        country = 'Germany'
    return country


def format_tweet(country, lat, lon, day, month, year, false_col=False):
    '''
    Given a country, latitude and longitude, this will format a tweet based on the preview file downloaded by
    grabba_grabba
    :param country: Country
    :param lat: Latitude
    :param lon: Longitude
    :param day: Integer day
    :param month: Integer month
    :param year: Integer year
    :param false_col: Whether the image is in false colour or not
    :return: formatted tweet
    '''
    month = calendar.month_name[month]
    if 4 <= day <= 20 or 24 <= day <= 30:
        suffix = "th"
    else:
        suffix = ["st", "nd", "rd"][day % 10 - 1]

    if country:
        country = str(country).replace(' ', '')
        country = country.replace("'", "")
        country = country.replace("-", "")
        country = assign_country_shortnames(country)
        if false_col == True:
            tweet = 'NIR-R-G Image of %s (%.2f, %.2f) from the %d%s of %s, %s #ESA #EU #%s #Sentinel #space %s' % (
                str(country), lat, lon, day, suffix, month, year, ''.join(str(country).split(' ')),
                settings.MAP_URL % (lat, lon))
        else:
            tweet = 'Image of %s (%.2f, %.2f) from the %d%s of %s, %s #ESA #EU #%s #Sentinel #space %s' % (
                str(country), lat, lon, day, suffix, month, year, ''.join(str(country).split(' ')),
                settings.MAP_URL % (lat, lon))
    else:
        tweet = 'Image of International space (%.2f, %.2f), from the %d%s of %s, %s' % (
            lat, lon, day, suffix, month, year)
    return tweet


def assign_country_shortnames(country):
    """
    Shorten some of the longer country names
    :param country: country name
    :return: shortened country name
    """
    if 'States' in country:
        country = 'USA'
    if 'Ivoire' in country:
        country = 'Ivory Coast'
    if 'Micronesia' in country:
        country = 'Micronesia'
    if 'Papua' in country:
        country = 'Papua NG'
    if 'South Africa' in country:
        country = 'RSA'
    if 'African' in country:
        country = 'CAR'
    if 'Caribbean' in country:
        country = 'The Caribbean'
    if 'Austrail' in country:
        country = 'Australia'
    if 'Congo' in country:
        country = 'The Congo'
    if 'Myanmar' in country:
        country = 'Myanmar'
    return country


def get_valid_lat_lon():
    """
    Sample a random point on the earth - bias towards the tropics
    :return: latitude and longitude as floats
    """
    lat = np.random.normal(0, 0.99) * 90
    while np.abs(lat) > 85:
        lat = np.random.normal(0, 0.99) * 90
    lon = (np.random.random() * 2 - 1) * 180
    return lat, lon


def clean_up_directory():
    """
    Cleans the data used for the previous tweet
    """
    shutil.rmtree('output')
    os.remove('output.zip')
    os.remove('image.jpg')


def tweet_current_image(boa, country, false_colour, im_date, lat, lon, start_time):
    """
    Tweets the current image given the metadata
    :param boa: True if the scene is atmospherically corrected
    :param country: country screen
    :param false_colour: Boolean of whether the image is in false colour
    :param im_date: date string
    :param lat: latitude float
    :param lon: longitude float
    :param start_time: time that the last tweet was uploaded
    :return: None
    """
    auth = tweepy.OAuthHandler(settings.CONSUMER_KEY, settings.CONSUMER_SECRET)
    auth.set_access_token(settings.ACCESS_KEY, settings.ACCESS_SECRET)
    if start_time:
        wait_one_hour_from_last_tweet(start_time)
    api = tweepy.API(auth)
    tweet = format_tweet(country, lat, lon, int(im_date.split('-')[2]), int(im_date.split('-')[1]),
                         int(im_date.split('-')[0]), false_col=false_colour)
    if boa:
        tweet = tweet.replace('#ESA', '(atm corr) #ESA')
    api.update_with_media('image.jpg', status=tweet)


def wait_one_hour_from_last_tweet(start_time):
    """
    Waits an hour from given start time
    :param start_time: datetime of last tweet
    :return: None
    """
    delta = (datetime.datetime.now() - start_time).seconds
    while delta < 3600:
        delta = (datetime.datetime.now() - start_time).seconds
        print('last tweet was %d seconds ago' % (delta))
        time.sleep(10)


def colour_balance_image(image):
    """
    Take a numpy image and colour balance it as an 8 bit integer
    :param image: image array (width x height x bands)
    :return: PIL Image
    """
    image = ma.masked_equal(image, 0)
    image = (image / (image.max() / 255.))
    mean = image.mean()
    scalar = 125 / mean
    image = image * scalar
    image = image.clip(0, 255)
    im = Image.fromarray(image.astype(np.uint8))
    return im


def read_bands(bands, fp):
    """
    Reads the given bands into memory
    :param bands: Band names
    :param fp: Filepath + file prefix
    :return: The (width x height x bands) image as an array
    """
    image = np.dstack([rio.open(fp + band).read(1, out_shape=(2196, 2196)) for band in bands])
    return image


def retrieve_bands_for_image():
    """
    Get the file paths of the bands using glob regexing
    :return: bands (names), atmospheric correction as a boolean, false colour boolean, file prefix
    """
    boa = False
    bands = ['B04.jp2', 'B03.jp2', 'B02.jp2']
    fc = np.random.random() > 0.8
    if fc:
        bands = ['B08.jp2', 'B04.jp2', 'B03.jp2']
    fp = glob2.glob('output/*/GRANULE/*/IMG_DATA/*')[0].split('.jp2')[0][:-3]
    if fp.split('/')[-1] == 'R':
        fp = glob2.glob('output/*/GRANULE/*/IMG_DATA/R10m/*')[0].split('_10m.jp2')[0][:-3]
        bands = ['B04_10m.jp2', 'B03_10m.jp2', 'B02_10m.jp2']
        boa = True
        if fc:
            bands = ['B08_10m.jp2', 'B04_10m.jp2', 'B03_10m.jp2']
    return bands, boa, fc, fp


def unzip_tile():
    """
    Unzips the current tile in the directory
    :return: None
    """
    if not os.path.exists('output'):
        os.mkdir('output')
    with zipfile.ZipFile('output.zip', 'r') as zip_ref:
        zip_ref.extractall('./output')


def download_tile(auth, product_link):
    """
    Downloads the given tile to the directory
    :param auth: Scihub authentication credentials
    :param product_link: Link to the tile to download
    :return: None
    """
    response = requests.get(product_link, stream=True, auth=auth)
    with open('output.zip', 'wb') as handle:
        for block in response.iter_content(1024):
            handle.write(block)


def get_coordinates_and_country():
    """
    Randomly search coordinates, return the country and coordinates once a search is successful
    :return: country in which the coordinates lie and the coordinates
    """
    country = None
    while country is None:
        lat, lon = get_valid_lat_lon()
        print(lat, lon)
        try:
            country = get_country_osm(lat, lon)
        except:
            print('No country found for these coordinates')
            continue
        time.sleep(0.3)
    return country, lat, lon


def get_random_date():
    """
    Gets a random datetime
    :return: the datetime
    """
    d1 = datetime.datetime(2016, 1, 1)
    d2 = datetime.datetime.now()
    rd = random_date(d1, d2)
    return rd


def get_image_from_search(res_json):
    """
    Gets metadata from scihub search result
    :param res_json: json of the search response
    :return: image date, preview url, product url
    """
    first_res = res_json['feed']['entry'] if res_json['feed']['opensearch:totalResults'] is '1' else \
        check_for_atm_corr(res_json)
    preview = first_res['link'][2]['href']
    product_link = first_res['link'][0]['href']
    im_date = first_res['summary'].split(',')[0].split('T')[0].split(' ')[1]
    return im_date, preview, product_link


def check_for_atm_corr(res_json):
    """
    Checks if any of the results are L2A (Atmospherically corrected)
    :param res_json: json response of search
    :return: First atmospherically corrected result, else the first L1C result
    """
    l2a_results = [res for res in res_json['feed']['entry'] if 'L2A' in res['title']]
    if not l2a_results:
        return res_json['feed']['entry'][0]
    else:
        return l2a_results[0]


if __name__ == '__main__':
    run_bot()
