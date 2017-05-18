#!/usr/bin/env python
# encoding: utf-8

import calendar
import json
import time
from datetime import timedelta
from random import randrange
from urllib.request import urlopen

import cv2
import glymur
import numpy as np
import numpy.ma as ma
import tweepy  # https://github.com/tweepy/tweepy
from geopy.geocoders import Nominatim
from tqdm import *

from grabba_grabba_hey import *

screen_name = 'sentinel_bot'
consumer_key = 'Your key'  # keep the quotes, replace this with your consumer key
consumer_secret = 'Your secret'  # keep the quotes, replace this with your consumer secret key
access_key = 'Access key'  # keep the quotes, replace this with your access token
access_secret = 'Access secret'  # keep the quotes, replace this with your access token secret


def get_all_tweets(screen_name, consumer_key, consumer_secret, access_key, access_secret):
    '''
	:param screen_name: Screen name of twitter account you want to query
	:param consumer_key: Provided by twitter
	:param consumer_secret: Provided by twitter
	:param access_key: Provided by twitter
	:param access_secret: Provided by twitter
	:return: Writes a csv file containing last ~3,000 tweets from a specific used
	'''

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)
    # Twitter only allows access to a users most recent 3240 tweets with this method
    # initialize a list to hold all the tweepy Tweets
    alltweets = []

    # make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.user_timeline(screen_name=screen_name, count=200)

    # save most recent tweets
    alltweets.extend(new_tweets)

    # save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1

    # keep grabbing tweets until there are no tweets left to grab
    print('Retrieving tweets')
    while len(new_tweets) > 0:
        # all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(screen_name=screen_name, count=200, max_id=oldest)

        # save most recent tweets
        alltweets.extend(new_tweets)

        # update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1

    # transform the tweepy tweets into a 2D array
    latest_tweets = [str(tweet.text) for tweet in alltweets]
    print('Tweets retrieved, starting random search...')
    return np.array(latest_tweets)


def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects and a date one month in the future of that datetime.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second), start + timedelta(seconds=random_second + (30 * 24 * 60 * 60))


def get_country_google(lat, lon):
    '''
    Returns the country parameter from a queried lat, lon using google's API
    :param lat: Latitiude to query
    :param lon: Longitude to query
    :return: Country of lat,lon according to google maps' geolocater
    '''
    url = "http://maps.googleapis.com/maps/api/geocode/json?"
    url += "latlng=%s,%s&sensor=false" % (lat, lon)
    v = urlopen(url).read()
    j = json.loads(str(v).replace('\\n', '').replace(' ', '')[2:-1])
    components = j['results'][0]['address_components']
    country = town = None
    for c in components:
        if "country" in c['types']:
            country = c['long_name']
        if "postal_town" in c['types']:
            town = c['long_name']
    return country


def get_country_osm(lat, lon):
    '''
    Returns the country parameter from a queried lat, lon using OSM's API - useful backup for when limits are exceeded/down
    :param lat: Latitiude to query
    :param lon: Longitude to query
    :return: Country of lat,lon according to OSMs' geolocater
    '''
    osm_geolocater = Nominatim()
    loc = osm_geolocater.reverse((lat, lon), language='en')
    country = str(loc.raw['address']['country'])
    if country == 'Mexicanos':
        country = 'Mexico'
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


def checkifdone(tweet, tweets):
    '''
    :param tweet: Tweet which is loaded and ready for distribution
    :param tweets: Array of the ~3000 most recent tweets from the bot
    :return: True if the tweet is not within the last ~3,000 tweets and hasn't been posted within the last week, otherwise False
    '''
    raw_tweets = []
    for i in range(len(tweets)):
        try:
            raw_tweets.append(
                tweets[i].split('http')[0][:-1].split('(')[0][:-1] + tweets[i].split('http')[0][:-1].split(')')[1])
        except:
            continue
    tweet_test = tweet.split('http')[0][:-1].split('(')[0][:-1] + tweet.split('http')[0][:-1].split(')')[1]
    if tweet_test in raw_tweets:
        return True
    else:
        return False


def get_place_check_valid():
    '''
    Downloads preview data if the condition is True
    :return: Returns a country and date where initial tests are passed
    '''

    count = 0
    searching = True
    while searching:
        lat = np.random.normal(0, 0.99) * 90
        lon = (np.random.random() * 2 - 1) * 180
        try:
            country = get_country_google(lat, lon)
            time.sleep(2)
            if 'water' in country:
                count += 2
                continue
        except:
            try:
                country = get_country_osm(lat, lon)
                time.sleep(1)
                if 'water' in country:
                    count += 2
                    continue
            except:
                count += 2
                continue
        d1 = datetime.datetime.strptime('1/1/2016', '%m/%d/%Y')
        d2 = datetime.datetime.strptime(
            str(datetime.datetime.now().month) + '/' + str(datetime.datetime.now().day) + '/' + str(
                datetime.datetime.now().year), '%m/%d/%Y')
        rd, rd2 = random_date(d1, d2)
        try:
            out = download_sentinel_amazontest(lat, lon, rd, 'out_data/', end_date=rd2)
        except:
            continue
        if out == 6:
            return country, rd, rd2, lat, lon


def format_tweet(country, lat, lon):
    '''
    Given a country, latitude and longitude, this will format a tweet based on the preview file downloaded by
    grabba_grabba
    :param country: Country
    :param lat: Latitude
    :param lon: Longitude
    :return:
    '''
    month_to_int = dict((v, k) for k, v in enumerate(calendar.month_abbr))
    fp = [x[0] for x in os.walk(os.getcwd())][-2]  # The filepaths where preview images have been stored
    day = int(fp.split('\\')[-2])
    month = int(fp.split('\\')[-3])
    year = int(fp.split('\\')[-4])
    month = calendar.month_name[month]
    if 4 <= day <= 20 or 24 <= day <= 30:
        suffix = "th"
    else:
        suffix = ["st", "nd", "rd"][day % 10 - 1]

    if country:
        country = str(country).replace(' ', '')
        country = country.replace("'", "")
        country = country.replace("-", "")  # hacky formatting, need to fix, individual country level \
                                            # corrections are in order to make it typesafe/under char limit for twitter
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
        tweet = 'Image of %s (%.2f, %.2f) from the %d%s of %s, %s #ESA #EU #%s #Sentinel #space' % (
            str(country), lat, lon, day, suffix, month, year, ''.join(str(country).split(' ')))
    else:
        tweet = 'Image of International space (%.2f, %.2f), from the %d%s of %s, %s' % (
            lat, lon, day, suffix, month, year)
    return tweet, fp, int(day), int(month_to_int[month[:3]]), int(year)


def image_check(test, lat, lon, rd, rd2):
    '''
    Because these images are so big, we download a small jpeg preview and run some quick tests to make sure the scenes
    aren't too cloudy/are somewhat colourful. This is very subjective and tweakable.
    :param test: The loaded jpeg preview
    :param lat: Latitude
    :param lon: Longitude
    :param rd: Start date of search time
    :param rd2: End date of search time
    :return:
    '''

    contrast = [ma.std(ma.masked_greater(test[:, :, 0], 230)), ma.std(ma.masked_greater(test[:, :, 1], 230)),
                ma.std(ma.masked_greater(test[:, :, 2], 230))]
    cc = 0
    for i in contrast:
        if i > 20:
            cc += 3
    if cc < 2:
        print("Not enough contrast")
        os.chdir('../../../../../../../../')
        os.system('rm -r out_data')
        return False
    if np.where(test < 230)[0].size < test.size * .96:
        print(r"Not enough unmasked/too bright")
        os.chdir('../../../../../../../../')
        os.system('rm -r out_data')
        return False
    if test.mean() < 90:
        print("Mean too low")
        os.chdir('../../../../../../../../')
        os.system('rm -r out_data')
        return False
    else:
        os.chdir('../../../../../../../../')
        try:
            print('Downloading raw data...')
            out = download_sentinel_amazon(lat, lon, rd, 'out_data/', end_date=rd2)  # download the data
            if out == 6:
                return True
        except:
            os.system('rm -r out_data')
            return False


def prepare_image():
    rsize = os.stat('B04.jp2')
    if int(rsize.st_size) < 13070000:
        os.system('rm -r out_data')
        return 0, False
    r = glymur.Jp2k('B04.jp2')[:]
    b = glymur.Jp2k('B02.jp2')[:]
    g = glymur.Jp2k('B03.jp2')[:]
    image = np.zeros((r.shape[0], r.shape[1], 3))
    image[:, :, 0] = b
    image[:, :, 1] = g
    image[:, :, 2] = r
    image = (image / (image.max() / 255.))
    mean = image.mean()
    scalar = 125 / mean
    image = image * scalar
    image = image.clip(0, 255)
    image = cv2.resize(image, (2196, 2196))
    return image, True


def sentbot(screen_name, consumer_key, consumer_secret, access_key, access_secret):
    bot_on = True
    delay_tweet = False
    reload = True

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    while bot_on:

        time.sleep(1)  # Need to delay requests to geolocaters in order to stay under daily limit!
        if reload:
            tweets = get_all_tweets(screen_name, consumer_key, consumer_secret, access_key,
                                    access_secret)  # grabs latest tweets
        reload = False  # Don't need to reload tweets unless we have posted a new one

        if not os.path.isdir('out_data'):
            os.mkdir('out_data')  # where we will store the output

        country, rd1, rd2, lat, lon = get_place_check_valid()  # generate lat/lon + dates, grab the country name

        tweet, fp, day, month, year = format_tweet(country, lat, lon)
        print('Found image in %s on day:%s, month:%s, year:%s, testing preview' % (country, day, month, year))
        done = checkifdone(tweet, tweets)

        if done:
            print('This tweets come up recently, restarting loop')
            os.system('rm -r out_data')
            continue

        date_of_image = datetime.datetime(year, month, day)
        os.chdir(fp)
        test = cv2.imread(
            'preview.jpg')  # Before downloading the full size images, do some check checks for clouds/colour
        print('Checking image contrast/quality...')
        image_check_test = image_check(test, lat, lon, date_of_image - timedelta(days=1),
                                       date_of_image + timedelta(days=1))

        if image_check_test == False:  # Continue if the test fails
            continue

        os.chdir(fp)
        image, passed = prepare_image()
        if passed == False:
            continue
        os.chdir('../../../../../../../../')
        cv2.imwrite('out.jpg', image.astype(np.uint8))
        fsize = os.stat('out.jpg')

        while int(fsize.st_size) > 3070000:  # Keep resizing until the media file is <3 mb
            im = cv2.imread('out.jpg')
            cv2.imwrite('out.jpg', cv2.resize(im, (0, 0), fx=0.98, fy=0.98))
            fsize = os.stat('out.jpg')
        if delay_tweet:
            wait_time = 1800 - (datetime.datetime.now() - starttime).seconds
            if wait_time < 1:
                ''
            else:
                for i in tqdm(range(wait_time)):
                    time.sleep(1)  # Tweet every 15 minutes
        delay_tweet = False
        try:
            api.update_with_media('out.jpg', status=tweet)
            starttime = datetime.datetime.now()
            delay_tweet = True
        except:
            print('tweet failed')
            continue
        print('reached here')
        reload = True
        os.system('rm -r out_data')


if __name__ == '__main__':
    sentbot(screen_name, consumer_key, consumer_secret, access_key, access_secret)
