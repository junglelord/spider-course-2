import re
from lxml import etree

f = open('List of Google domains - Wikipedia.htm', 'rb+')
html = f.read()
f.close()

tree = etree.HTML(html)

google_links = []

external_links = tree.xpath('//td/span/a[@class="external text"]')

for external_link in external_links:
    link_str = external_link.attrib['href']
    if link_str.find('http://google.') != -1:
        google_links.append(link_str[7:])

print '[\"' + '\",\"'.join(google_links) + '\"]'

