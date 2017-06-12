# -*- coding: utf-8 -*-

import urllib2
import httplib
import re
from redis_manager import RedisTermsManager

class CrawlBSF:
    request_headers = {
        'host': "zh.wikipedia.org",
        'connection': "keep-alive",
        'cache-control': "no-cache",
        'upgrade-insecure-requests': "1",
        'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36",
        'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        'accept-language': "zh-CN,en-US;q=0.8,en;q=0.6"
        }

    dir_name = 'wiki_category'
    downloaded_urls = []

    re_articles = re.compile('<a href="/wiki/(?!Category:)[^/>]*>(.*?)<', re.S|re.M)

    category_html_file_name = dir_name + u'category.txt'

    wiki_category_base_url = 'https://zh.wikipedia.org/wiki/Category:'

    cate_name_index = wiki_category_base_url.rfind(':')

    redis_mgr = RedisTermsManager()

    def __init__(self, category):
        self.redis_mgr.enqueue_item(self.redis_mgr.get_category_list_name(), category)

    def getpagecontent(self, category):
        print "downloading %s" % (category)
        url = self.wiki_category_base_url + category
        try:
            req = urllib2.Request(url, headers=self.request_headers)
            response = urllib2.urlopen(req)
            html_page = response.read()
            filename = "%s/%s.html" % (self.dir_name, category)
            fo = open( filename.decode('utf-8'), 'wb+')
            fo.write(html_page)
            fo.close()
        except urllib2.HTTPError, Arguments:
            print Arguments
            return
        except httplib.BadStatusLine:
            print 'BadStatusLine'
            return
        except IOError as e:
            print "I/O error({0}): {1}".format(e.errno, e.strerror)
            return
        except Exception, Arguments:
            print Arguments
            return

        articles = self.re_articles.findall(html_page)

        for article in articles:
            self.redis_mgr.enqueue_item(self.redis_mgr.get_article_list_name(), article)

    def start_crawl(self):
        while True:
            category = self.redis_mgr.dequeue_item(self.redis_mgr.get_category_list_name())
            if category is None:
                break
            self.getpagecontent(category)
        # self.redis_mgr.clear()

crawler = CrawlBSF(u"诺贝尔物理学奖获得者")
crawler.start_crawl()