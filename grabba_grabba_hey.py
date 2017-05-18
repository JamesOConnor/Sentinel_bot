#!/usr/bin/env python
"""
A simple interface to download Sentinel-1 and Sentinel-2 datasets from
the COPERNICUS Sentinel Hub.
"""
from functools import partial
import hashlib
import os
import datetime
import sys
import xml.etree.cElementTree as ET
import re

import requests
from concurrent import futures


# hub_url = "https://scihub.copernicus.eu/dhus/search?q="
hub_url = "https://scihub.copernicus.eu/apihub/search?q="
MGRS_CONVERT = "http://legallandconverter.com/cgi-bin/shopmgrs3.cgi"
aws_url = 'http://sentinel-s2-l1c.s3.amazonaws.com/?delimiter=/&prefix=tiles/'
aws_url_dload = 'http://sentinel-s2-l1c.s3.amazonaws.com/'
requests.packages.urllib3.disable_warnings()


def get_mgrs(longitude, latitude):
    """A method that uses a website to infer the Military Grid Reference System
    tile that is used by the Amazon data buckets from the latitude/longitude
    Parameters
    -------------
    longitude: float
        The longitude in decimal degrees
    latitude: float
        The latitude in decimal degrees
    Returns
    --------
    The MGRS tile (e.g. 29TNJ)
    """
    r = requests.post(MGRS_CONVERT,
                      data=dict(latitude=longitude,
                                longitude=latitude, xcmd="Calc", cmd="gps"))
    for liner in r.text.split("\n"):
        if liner.find("<title>") >= 0:
            mgrs_tile = liner.replace("<title>", "").replace("</title>", "")
            mgrs_tile = mgrs_tile.replace(" ", "")
    try:
        return mgrs_tile[:5]  # This should be enough
    except NameError:
        return None


def calculate_md5(fname):
    hasher = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest().upper()


def do_query(query, user="guest", passwd="guest"):
    """
    A simple function to pass a query to the Sentinel scihub website. If
    successful this function will return the XML file back for further
    processing.
    query: str
        A query string, such as "https://scihub.copernicus.eu/dhus/odata/v1/"
        "Products?$orderby=IngestionDate%20desc&$top=100&$skip=100"
    Returns:
        The relevant XML file, or raises error
    """
    r = requests.get(query, auth=(user, passwd), verify=False)
    if r.status_code == 200:
        return r.text
    else:
        raise IOError("Something went wrong! Error code %d" % r.status_code)


def download_product(source, target, user="guest", passwd="guest"):
    """
    Download a product from the SentinelScihub site, and save it to a named
    Download a product from the SentinelScihub site, and save it to a named
    local disk location given by ``target``.
    source: str
        A product fully qualified URL
    target: str
        A filename where to download the URL specified
    """
    md5_source = source.replace("$value", "/Checksum/Value/$value")
    r = requests.get(md5_source, auth=(user, passwd), verify=False)
    md5 = r.text
    if os.path.exists(target):
        md5_file = calculate_md5(target)
        if md5 == md5_file:
            return
    chunks = 65536  # 1048576 # 1MiB...
    while True:
        r = requests.get(source, auth=(user, passwd), stream=True,
                         verify=False)
        if not r.ok:
            raise IOError("Can't start download... [%s]" % source)
        file_size = int(r.headers['content-length'])
        print( "Downloading to -> %s" % target)
        print( "%d bytes..." % file_size)
        with open(target, 'wb') as fp:
            cntr = 0
            dload = 0
            for chunk in r.iter_content(chunk_size=chunks):
                if chunk:
                    cntr += 1
                    if cntr > 100:
                        dload += cntr * chunks
                        print( "\tWriting %d/%d [%5.2f %%]" % (dload, file_size,
                                                              100. * float(dload) / \
                                                              float(file_size)))
                        sys.stdout.flush()
                        cntr = 0

                    fp.write(chunk)
                    fp.flush()
                    os.fsync(fp)

        md5_file = calculate_md5(target)
        if md5_file == md5:
            break
        return


def parse_xml(xml):
    """
    Parse an OData XML file to havest some relevant information re products
    available and so on. It will return a list of dictionaries, with one
    dictionary per product returned from the query. Each dicionary will have a
    number of keys (see ``fields_of_interest``), as well as ``link`` and
    ``qui
    """
    fields_of_interest = ["filename", "identifier", "instrumentshortname",
                          "orbitnumber", "orbitdirection", "producttype",
                          "beginposition", "endposition"]
    tree = ET.ElementTree(ET.fromstring(xml))
    # Search for all the acquired images...
    granules = []
    for elem in tree.iter(tag="{http://www.w3.org/2005/Atom}entry"):
        granule = {}
        for img in elem.getchildren():
            if img.tag.find("id") >= 0:
                granule['id'] = img.text
            if img.tag.find("link") and img.attrib.has_key("href"):

                if img.attrib['href'].find("Quicklook") >= 0:
                    granule['quicklook'] = img.attrib['href']
                elif img.attrib['href'].find("$value") >= 0:
                    granule['link'] = img.attrib['href'].replace("$value", "")

            if img.attrib.has_key("name"):
                if img.attrib['name'] in fields_of_interest:
                    granule[img.attrib['name']] = img.text

        granules.append(granule)

    return granules
    # print( img.tag, img.attrib, img.text)
    # for x in img.getchildren():


def download_sentinel(location, input_start_date, input_sensor, output_dir,
                      input_end_date=None, username="guest", password="guest"):
    input_sensor = input_sensor.upper()
    sensor_list = ["S1", "S2"]
    if not input_sensor in sensor_list:
        raise ValueError("Sensor can only be S1 or S2. You provided %s"
                         % input_sensor)
    else:
        sensor_str = 'filename:%s*' % input_sensor.upper()

    try:
        start_date = datetime.datetime.strptime(input_start_date,
                                                "%Y.%m.%d").isoformat()
    except ValueError:
        try:
            start_date = datetime.datetime.strptime(input_start_date,
                                                    "%Y-%m-%d").isoformat()
        except ValueError:
            start_date = datetime.datetime.strptime(input_start_date,
                                                    "%Y/%j").isoformat()
    start_date = start_date + "Z"

    if input_end_date is None:
        end_date = "NOW"
    else:
        try:
            end_date = datetime.datetime.strptime(input_end_date,
                                                  "%Y.%m.%d").isoformat()
        except ValueError:
            try:
                end_date = datetime.datetime.strptime(input_end_date,
                                                      "%Y-%m-%d").isoformat()
            except ValueError:
                end_date = datetime.datetime.strptime(input_end_date,
                                                      "%Y/%j").isoformat()

    if len(location) == 2:
        location_str = 'footprint:"Intersects(%f, %f)"' % (location[0], location[1])
    elif len(location) == 4:
        location_str = 'footprint:"Intersects( POLYGON(( " + \
            "%f %f, %f %f, %f %f, %f %f, %f %f) ))"' % (
            location[0], location[0],
            location[0], location[1],
            location[1], location[1],
            location[1], location[0],
            location[0], location[0])

    time_str = 'beginposition:[%s TO %s]' % (start_date, end_date)

    query = "%s AND %s AND %s" % (location_str, time_str, sensor_str)
    query = "%s%s" % (hub_url, query)
    # query = "%s%s" % ( hub_url, urllib2.quote(query ) )

    result = do_query(query, user=username, passwd=password)
    granules = parse_xml(result)

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    ret_files = []
    for granule in granules:
        download_product(granule['link'] + "$value", os.path.join(output_dir,
                                                                  granule['filename'].replace("SAFE", "zip")))
        ret_files.append(os.path.join(output_dir,
                                      granule['filename'].replace("SAFE", "zip")))

    return granules, ret_files


def parse_aws_xml(xml_text):

    tree = ET.ElementTree(ET.fromstring(xml_text))
    files_to_get = []
    for elem in tree.iter():
        for k in elem.getchildren():
            if k.tag.find ("Key") >= 0:
                if k.text.find ("tiles") >= 0:
                    files_to_get.append( k.text )
    return files_to_get

def aws_grabber(url, output_dir):
    output_fname = os.path.join(output_dir, url.split("tiles/")[-1])
    if not os.path.exists(os.path.dirname (output_fname)):
        # We should never get here, as the directory should always exist 
        # Note that in parallel, this can sometimes create a race condition
        # Groan
        os.makedirs (os.path.dirname(output_fname))
    with open(output_fname, 'wb') as fp:
        while True:
            try:
                r = requests.get(url, stream=True)
                break
            except requests.execeptions.ConnectionError:
                time.sleep ( 240 )
        for block in r.iter_content(8192):
            fp.write(block)
    return output_fname


def download_sentinel_amazon(longitude, latitude, start_date, output_dir,
                             end_date=None, n_threads=15):
    """A method to download data from the Amazon cloud """
    # First, we get hold of the MGRS reference...
    mgrs_reference = get_mgrs(longitude, latitude)
    utm_code = mgrs_reference[:2]
    lat_band = mgrs_reference[2]
    square = mgrs_reference[3:]

    front_url = aws_url + "%s/%s/%s" % (utm_code, lat_band, square)
    this_date = start_date
    one_day = datetime.timedelta(days=1)
    files_to_download = []
    if end_date is None:
        end_date = datetime.datetime.today()
    while this_date <= end_date:

        the_url = "{0}{1}".format(front_url, "/{0:d}/{1:d}/{2:d}/0/".format(
            this_date.year, this_date.month, this_date.day))
        r = requests.get(the_url)
        rqi = requests.get(the_url + "qi/")
        raux = requests.get(the_url + "aux/")
        more_files = ( parse_aws_xml(r.text) +
                      parse_aws_xml(rqi.text) + 
                      parse_aws_xml(raux.text) )
        if len(more_files) > 0:
            files_to_download.extend ( more_files )
        this_date += one_day
    inds=[]
    for n,i in enumerate(files_to_download):
        if 'B02.j' in i:
            inds.append(i)
        if 'B03.j' in i:
            inds.append(i)
        if 'B04.j' in i:
            inds.append(i)
    files_to_download=inds
    the_urls = []
    for fich in files_to_download:
        the_urls.append(aws_url_dload + fich)
        ootput_dir = os.path.dirname ( os.path.join(output_dir, 
                                                    fich.split("tiles/")[-1]))
        if not os.path.exists ( ootput_dir ):
            os.makedirs ( ootput_dir )
    ok_files = []
    download_granule_patch = partial(aws_grabber, output_dir=output_dir)
    with futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
        for fich in executor.map(download_granule_patch, the_urls):
            ok_files.append(fich)
    if the_urls==[]:
        ''
    else:
        return 6


def download_sentinel_amazontest(longitude, latitude, start_date, output_dir,
                             end_date=None, n_threads=15):
    """A method to download data from the Amazon cloud """
    # First, we get hold of the MGRS reference...
    mgrs_reference = get_mgrs(longitude, latitude)
    utm_code = mgrs_reference[:2]
    lat_band = mgrs_reference[2]
    square = mgrs_reference[3:]

    front_url = aws_url + "%s/%s/%s" % (utm_code, lat_band, square)
    this_date = start_date
    one_day = datetime.timedelta(days=1)
    files_to_download = []
    if end_date is None:
        end_date = datetime.datetime.today()
    while this_date <= end_date:

        the_url = "{0}{1}".format(front_url, "/{0:d}/{1:d}/{2:d}/0/".format(
            this_date.year, this_date.month, this_date.day))
        r = requests.get(the_url)
        rqi = requests.get(the_url + "qi/")
        raux = requests.get(the_url + "aux/")
        more_files = ( parse_aws_xml(r.text) +
                      parse_aws_xml(rqi.text) + 
                      parse_aws_xml(raux.text) )
        if len(more_files) > 0:
            files_to_download.extend ( more_files )
        this_date += one_day
    inds=[]
    for n,i in enumerate(files_to_download):
        if 'preview.jpg' in i:
            inds.append(i)
    files_to_download=inds
    the_urls = []
    for fich in files_to_download:
        the_urls.append(aws_url_dload + fich)
        ootput_dir = os.path.dirname ( os.path.join(output_dir, 
                                                    fich.split("tiles/")[-1]))
        if not os.path.exists ( ootput_dir ):
            os.makedirs ( ootput_dir )
    ok_files = []
    download_granule_patch = partial(aws_grabber, output_dir=output_dir)
    with futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
        for fich in executor.map(download_granule_patch, the_urls):
            ok_files.append(fich)
    if the_urls==[]:
        ''
    else:
        return 6
