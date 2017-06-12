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

    dir_name = 'wiki_article'

    re_categories = re.compile('<a href="/wiki/Category:[^>]*>(.*?)<', re.S|re.M)

    # 保存原始文件名
    article_html_file_name = dir_name + 'articles.txt'

    # base url
    wiki_article_base_url = 'https://zh.wikipedia.org/wiki/'

    cate_name_index = wiki_article_base_url.rfind('/')

    redis_mgr = RedisTermsManager()

    def __init__(self, article):
        self.redis_mgr.enqueue_item(self.redis_mgr.get_article_list_name(), article)

    def getpagecontent(self, article):
        print "downloading %s" % (article)
        # 构造抓取的url
        url = self.wiki_article_base_url + article
        try:
            # 发起网络请求，提取网页数据
            req = urllib2.Request(url, headers=self.request_headers)
            response = urllib2.urlopen(req)
            html_page = response.read()

            # 把网页存储到本地磁盘
            filename = "%s/%s.html" % (self.dir_name, article)
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

        # 通过正则表达式，提取所有的类别
        categories = self.re_categories.findall(html_page)

        for category in categories:
            self.redis_mgr.enqueue_item(self.redis_mgr.get_category_list_name(), category)

    def start_crawl(self):
        while True:
            article = self.redis_mgr.dequeue_item(self.redis_mgr.get_article_list_name())
            if article is None:
                break
            self.getpagecontent(article)

crawler = CrawlBSF(u"阿尔伯特·爱因斯坦")
crawler.start_crawl()