#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import urllib2
import datetime
import zipfile
import glob
import gzip

def download_archives():
    """ download zipped minutely data from www.forexite.com """

    directory = './archives'

    # zip_url % (YYYY, MM, DD, MM, YY)
    zip_url = 'http://www.forexite.com/free_forex_quotes/%s/%s/%s%s%s.zip'

    # filename % (YYYY, MM, DD)
    filename = '%s_%s_%s.zip'

    referer = 'http://www.forexite.com/free_forex_quotes/'
    user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) \
                  AppleWebKit/525.13 (KHTML, like Gecko) \
                  Chrome/0.2.149.27 Safari/525.13'
    accept_language = 'ja,en-us;q=0.7,en;q=0.3'

    years = range(2001, datetime.datetime.now().year+1)
    months = range(1, 12+1)
    days = range(1, 31+1)

    # create directory
    if not os.path.exists(directory):
        os.mkdir(directory)

    # set http headers
    opener = urllib2.build_opener()
    opener.addheaders = [('Referer', referer),
                         ('User-Agent', user_agent),
                         ('Accept-Language', accept_language)]

    # download
    for year in years:
        for month in months:
            for day in days:
                year_str = str(year)
                month_str = str(month).zfill(2)
                day_str = str(day).zfill(2)
                url = zip_url % \
                        (year_str, month_str, day_str, month_str, year_str[-2:])
                fn = '%s/%s' % (directory,
                                filename % (year_str, month_str, day_str))

                print url

                # check already exists
                if os.path.exists(fn) and zipfile.is_zipfile(fn):
                    continue

                try:
                    opened_url = opener.open(url)
                except Exception as exception:
                    print exception.message
                    opened_url.close()
                    continue

                # check 404
                if opened_url.headers['Content-Type'] == 'text/html':
                    opened_url.close()
                    continue

                open(fn, 'w').write(opened_url.read())
                opened_url.close()


def sort_and_complement_pair_data(data):
    """ sort pairwise price data by time.
        complement non-existent data using latest close price. """

    # unique and sort
    data = list(set(data))
    data.sort()

    # complement span
    time = datetime.datetime.strptime(data[0][:15], '%Y%m%d,%H%M%S')
    endtime = datetime.datetime.strptime(data[-1][:15], '%Y%m%d,%H%M%S')
    interval = datetime.timedelta(seconds=60)

    index = 0
    while time != endtime:
        if data[index].startswith(time.strftime('%Y%m%d,%H%M%S')):
            # exists
            data[index] += ','
            index = index + 1
        else:
            # not exits
            close_price = data[index-1].split(',')[-2]
            data.append('%s,%s,%s,%s,%s,*' % (time.strftime('%Y%m%d,%H%M%S'), \
                        close_price, close_price, close_price, close_price))
        time = time + interval
    data.sort()

    return data

def make_minutely_data():
    """ concatenate all downloaded minutely data and make PAIR.txt.gz """

    directory_archive = './archives'
    directory_save = './data'

    data = {}

    # create directory
    if not os.path.exists(directory_save):
        os.mkdir(directory_save)

    # load data from archive
    archives = glob.glob('%s/*.zip' % directory_archive)
    for archive in archives:
        zf = zipfile.ZipFile(archive)
        for line in zf.read(zf.namelist()[0]).splitlines():
            # line = "<TICKER>,<DTYYYYMMDD>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>"
            # data[<TICKER>] = "<DTYYYYMMDD>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>"
            pos = line.index(',')
            pair = line[:pos]
            if pair == '<TICKER>':
                continue
            prices = line[pos+1:]
            if pair not in data:
                data[pair] = []
            data[pair].append(prices)

    # save
    for pair in data.keys():
        data[pair] = sort_and_complement_pair_data(data[pair])
        filename = '%s/%s_1min.txt.gz' % (directory_save, pair)
        gzip.open(filename, 'w').write('\n'.join(data[pair]))


if __name__ == '__main__':
    download_archives()
    make_minutely_data()
