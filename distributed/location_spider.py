# -*- coding: utf-8 -*-
import gzip
import threading

import protocol_constants as pc

import time
import urllib2
from urllib import urlencode
from heartbeat_client import HeartBeatClient

from StringIO import StringIO

google_domains = ["google.com", "google.ac", "google.ad", "google.ae", "google.com.af", "google.com.ag",
                  "google.com.ai", "google.al", "google.am", "google.co.ao", "google.com.ar", "google.as", "google.at",
                  "google.com.au", "google.az", "google.ba", "google.com.bd", "google.be", "google.bf", "google.bg",
                  "google.com.bh", "google.bi", "google.bj", "google.com.bn", "google.com.bo", "google.com.br",
                  "google.bs", "google.bt", "google.co.bw", "google.by", "google.com.bz", "google.ca", "google.com.kh",
                  "google.cc", "google.cd", "google.cf", "google.cat", "google.cg", "google.ch", "google.ci",
                  "google.co.ck", "google.cl", "google.cm", "google.cn", "google.com.co", "google.co", "google.co.cr",
                  "google.com.cu", "google.cv", "google.cx", "google.com.cy", "google.cz", "google.de", "google.dj",
                  "google.dk", "google.dm", "google.com.do", "google.dz", "google.com.ec", "google.ee", "google.com.eg",
                  "google.es", "google.com.et", "google.eu", "google.fi", "google.com.fj", "google.fm", "google.fr",
                  "google.ga", "google.ge", "google.gf", "google.gg", "google.com.gh", "google.com.gi", "google.gl",
                  "google.gm", "google.gp", "google.gr", "google.com.gt", "google.gy", "google.com.hk", "google.hn",
                  "google.hr", "google.ht", "google.hu", "google.co.id", "google.iq", "google.ie", "google.co.il",
                  "google.im", "google.co.in", "google.io", "google.is", "google.it", "google.je", "google.com.jm",
                  "google.jo", "google.co.jp", "google.co.ke", "google.ki", "google.kg", "google.co.kr",
                  "google.com.kw", "google.kz", "google.la", "google.com.lb", "google.com.lc", "google.li", "google.lk",
                  "google.co.ls", "google.lt", "google.lu", "google.lv", "google.com.ly", "google.co.ma", "google.md",
                  "google.me", "google.mg", "google.mk", "google.ml", "google.com.mm", "google.mn", "google.ms",
                  "google.com.mt", "google.mu", "google.mv", "google.mw", "google.com.mx", "google.com.my",
                  "google.co.mz", "google.com.na", "google.ne", "google.nf", "google.com.ng", "google.com.ni",
                  "google.nl", "google.no", "google.com.np", "google.nr", "google.nu", "google.co.nz", "google.com.om",
                  "google.com.pk", "google.com.pa", "google.com.pe", "google.com.ph", "google.pl", "google.com.pg",
                  "google.pn", "google.com.pr", "google.ps", "google.pt", "google.com.py", "google.com.qa", "google.ro",
                  "google.rs", "google.ru", "google.rw", "google.com.sa", "google.com.sb", "google.sc", "google.se",
                  "google.com.sg", "google.sh", "google.si", "google.sk", "google.com.sl", "google.sn", "google.sm",
                  "google.so", "google.st", "google.sr", "google.com.sv", "google.td", "google.tg", "google.co.th",
                  "google.com.tj", "google.tk", "google.tl", "google.tm", "google.to", "google.tn", "google.com.tr",
                  "google.tt", "google.com.tw", "google.co.tz", "google.com.ua", "google.co.ug", "google.co.uk",
                  "google.us", "google.com.uy", "google.co.uz", "google.com.vc", "google.co.ve", "google.vg",
                  "google.co.vi", "google.com.vn", "google.vu", "google.ws", "google.co.za", "google.co.zm",
                  "google.co.zw", "google.org", "google.net"]

location_file_name = 'location_en_count.txt'

location_dir_name = 'location_source/'

headers = {
    'x-devtools-emulate-network-conditions-client-id': "33a30327-cd4d-4341-abdc-b054f68e295a",
    'x-devtools-request-id': "9681.320",
    'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
    'x-chrome-uma-enabled': "1",
    'x-client-data': "CKq1yQEIjbbJAQiitskBCMG2yQEI+pzKAQipncoB",
    'accept': "*/*",
    'referer': "https://www.google.com.hk/",
    'accept-encoding': "gzip, deflate, sdch, br",
    'avail-dictionary': "LiNh3Gcr",
    'accept-language': "zh-CN,en-US;q=0.8,en;q=0.6",
    'cache-control': "no-cache",
}

domain_index = 0

CRAWL_DELAY = 0.1

threads = []
max_threads = 3

def get_google_domain():
    global domain_index
    if domain_index == len(google_domains):
        domain_index = 0
    return google_domains[domain_index]

def get_google_search_result(key_word):
    print '%06d: downloading %s' % (location_count, key_word)

    query_data = {"safe": "strict",
                  "q": key_word,
                  "oq": key_word,
                  "gs_l": "serp.12...0.0.0.5573.0.0.0.0.0.0.0.0..0.0....0...1..64.serp..0.0.0.R_H7SxNRdNI"
                  }
    querystring = urlencode(query_data)

    url = domain + querystring

    req = urllib2.Request(url, headers=headers)
    response = urllib2.urlopen(req)
    if response.info().get('Content-Encoding').find('gzip') != -1:
        buf = StringIO(response.read())
        fzip = gzip.GzipFile(fileobj=buf)
        content = fzip.read()
    else:
        content = response.read()

    f = open( location_dir_name + (key_word.replace(' ', '_') + '.html').decode('utf-8'), 'wb+')
    f.write(content)
    f.close()

    time.sleep(CRAWL_DELAY)

hb_client = HeartBeatClient()

while True:
    try:
        hb_client.connect()
        break
    except IOError:
        time.sleep(CRAWL_DELAY)

while hb_client.server_status != pc.SHUTDOWN:
    locations = hb_client.get_target_items(pc.LOCATIONS)

    for key_word in locations:

        location_count += 1

        domain = 'https://www.' + google_domains[domain_index] + '/search?'

        for t in threads:
            if not t.is_alive():
                threads.remove(t)
        if len(threads) >= max_threads:
            time.sleep(CRAWL_DELAY)
            continue
        try:
            t = threading.Thread(target=get_google_search_result, name=None, args=(key_word,))
            threads.append(t)
            # set daemon so main thread can exit when receives ctrl-c
            t.setDaemon(True)
            t.start()
            time.sleep(CRAWL_DELAY)
        except Exception, Arguments:
            print Arguments