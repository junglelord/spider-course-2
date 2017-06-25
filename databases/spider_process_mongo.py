import urllib2
import httplib
from lxml import etree
import thread
import threading
import os
import time
from mongomgr import MongoUrlManager

from hdfs import *
from hdfs.util import HdfsError

request_headers = {
    'host': "www.mafengwo.cn",
    'connection': "keep-alive",
    'cache-control': "no-cache",
    'upgrade-insecure-requests': "1",
    'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36",
    'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    'accept-language': "zh-CN,en-US;q=0.8,en;q=0.6"
}

dir_path = './mafengwo'

def save_page_content(html, filename):
    if os.path.exists(dir_path) is False:
        os.mkdir(dir_path)
    fo = open("%s/%s.html" % (dir_path, filename), 'wb+')
    fo.write(html)
    fo.close()

def get_page_content(cur_url, depth):
    print "downloading %s at level %d" % (cur_url, depth)
    try:
        req = urllib2.Request(cur_url, headers=request_headers)
        response = urllib2.urlopen(req)
        html_page = response.read()
        filename = cur_url[7:].replace('/', '_')

        #Write page to local files system
        save_page_content(html_page, filename)

        # Write to HDFS
        # with hdfs_client.write('/htmls/mfw/%s.html' % (filename)) as writer:
        #     writer.write(html_page)

        dbmanager.finishUrl(cur_url)
    except urllib2.HTTPError, Arguments:
        print Arguments
        return
    except httplib.BadStatusLine, Arguments:
        print Arguments
        return
    except IOError, Arguments:
        print Arguments
        return
    except HdfsError, Arguments:
        print Arguments
    except Exception, Arguments:
        print Arguments
        return
    # print 'add ' + hashlib.md5(cur_url).hexdigest() + ' to list'

    html = etree.HTML(html_page.lower().decode('utf-8'))
    hrefs = html.xpath(u"//a")

    for href in hrefs:
        try:
            if 'href' in href.attrib:
                val = href.attrib['href']
                if val.find('javascript:') != -1:
                    continue
                if val.startswith('http://') is False:
                    if val.startswith('/'):
                        val = 'http://www.mafengwo.cn' + val
                    else:
                        continue
                if val[-1] == '/':
                    val = val[0:-1]
                dbmanager.enqueueUrl(val, 'new', depth + 1)
        except ValueError:
            continue


max_num_thread = 5
dbmanager = MongoUrlManager()

dbmanager.enqueueUrl("http://www.mafengwo.cn", 'new', 0)

start_time = time.time()
is_root_page = True
threads = []

CRAWL_DELAY = 0.6

# use hdfs to save pages
# hdfs_client = InsecureClient('http://54.223.92.169:50070', user='ec2-user')

while True:
    curtask = dbmanager.dequeueUrl()
    # Go on next level, before that, needs to wait all current level crawling done
    if curtask is None:
        print 'No task available!'
        for t in threads:
            t.join()
        break

    # looking for an empty thread from pool to crawl

    if is_root_page is True:
        get_page_content(curtask['_id'], curtask['depth'])
        is_root_page = False
    else:
        while True:
            # first remove all finished running threads
            for t in threads:
                if not t.is_alive():
                    threads.remove(t)
            if len(threads) >= max_num_thread:
                time.sleep(CRAWL_DELAY)
                continue
            try:
                t = threading.Thread(target=get_page_content, name=None, args=(curtask['_id'], curtask['depth']))
                threads.append(t)
                # set daemon so main thread can exit when receives ctrl-c
                t.setDaemon(True)
                t.start()
                time.sleep(CRAWL_DELAY)
                break
            except Exception as error:
                print 'Unable to start thread: ' + error.message
                break