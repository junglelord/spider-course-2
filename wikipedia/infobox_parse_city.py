# -*- coding: utf-8 -*-

import sys
from HTMLParser import HTMLParser
import json
import re
import os

from lxml import etree

reload(sys)

# directory which saves html files
dir_name = 'wiki_article/city/'

sys.setdefaultencoding('utf-8')

re_tr=re.compile('<tr>(.*?)</tr>',re.M|re.S)
re_th=re.compile('<th[^>]*>(.*?)</th>',re.M|re.S)
re_td=re.compile('<td[^>]*>(.*?)</td>',re.M|re.S)
re_ref=re.compile('<sup[^>]*>.*?</sup>')

fields={}
fields['infos']=1
fields['key_name']=1

html_parser = HTMLParser()

def extract_content(node_html):
    html_str = re.sub('<[^>]+>','',node_html)
    return html_parser.unescape(html_str).replace(u'\xa0', '').replace(u'\u2022', '').replace('\n', '')

for filename in os.listdir(dir_name):
    if not filename.endswith(".html"):
        continue

    # raw html content
    f = open( dir_name + filename, 'rb')
    html = f.read()
    f.close()

    tr = etree.HTML(html)

    # infobox node
    infobox_nodes = tr.xpath("//table[contains(@class,'infobox')]")

    if len(infobox_nodes) == 0:
        continue
    infobox_node = infobox_nodes[0]

    # all tr nodes in infobox
    infobox_tr_nodes = infobox_node.xpath("//tr")

    #print html
    infos=[]

    info_item = {}
    info_item['type'] = 'geo'
    els = infobox_node.xpath('//*[contains(@class,"latitude")]')
    if len(els) > 0:
        info_item['latitude'] = els[0].text
        info_item['longitude'] = infobox_node.xpath('//*[contains(@class,"longitude")]')[0].text

    infos.append(info_item)

    node_index = 0
    group_item = None
    for tr_item in infobox_node.iterchildren('tr'):
        # skip first row of title
        if node_index == 0:
            node_index += 1
            continue
        node_index += 1

        if tr_item.get('class') is None:
            continue
        # a new group
        elif tr_item.get('class') == 'mergedtoprow':
            td = []
            th = ''
            for tr_children_item in tr_item.iterchildren('th'):
                th = extract_content(etree.tostring(tr_children_item))

            for tr_children_item in tr_item.iterchildren('td'):
                td.append(extract_content(etree.tostring(tr_children_item)))

            # skip a row without head, normally it's picture or description
            if th is '':
                continue

            # group title
            if len(td) == 0 and len(th) > 0:
                if group_item is not None:
                    infos.append(group_item)
                group_item = {}
                group_item['type'] = 'group'
                group_item['group'] = []
                group_item['key'] = th
            else:
                if group_item is not None:
                    infos.append(group_item)
                group_item = None
                info_item = {}
                info_item['type'] = 'text'
                info_item['key'] = th
                info_item['text'] = td
                infos.append(info_item)
        elif tr_item.get('class') == 'mergedrow':
            td = []
            th = ''
            for tr_children_item in tr_item.iterchildren('td'):
                td.append(extract_content(etree.tostring(tr_children_item)))
            for tr_children_item in tr_item.iterchildren('th'):
                th = extract_content(etree.tostring(tr_children_item))
            if len(th) is 0:
                continue
            info_item = {}
            info_item['type'] = 'text'
            info_item['key'] = th
            info_item['text'] = td
            if group_item is not None:
                group_item['group'].append(info_item)
            else:
                infos.append(info_item)

    if group_item is not None:
        infos.append(group_item)

    print json.dumps(infos)