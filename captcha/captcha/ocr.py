# -*- coding: utf-8 -*-

import csv
import string
from PIL import Image
import pytesseract


def ocr(img):
    # 获取图片的像素数组
    pixdata = img.load()
    colors = {}
    # 统计字符颜色像素情况
    for y in range(img.size[1]):
        for x in range(img.size[0]):
            if colors.has_key(pixdata[x,y]):
                colors[pixdata[x, y]] += 1
            else:
                colors[pixdata[x,y]] = 1

    # 排名第一的是背景色，第二的是主要颜色
    colors = sorted(colors.items(), key=lambda d:d[1], reverse=True)
    significant = colors[1][0]
    for y in range(img.size[1]):
        for x in range(img.size[0]):
            if pixdata[x,y] != significant:
                pixdata[x,y] = (255,255,255)
            else:
                pixdata[x, y] = (0,0,0)
    img.save('bw.png')
    # threshold the image to ignore background and keep text
    # gray = img.convert('L')
    # bw = gray.point(lambda x: 0 if x < 1 else 255, '1')
    # bw.save('captcha_gray.png')
    word = pytesseract.image_to_string(img, lang='eng', config='ocr.conf')
    ascii_word = ''.join(c for c in word if c in string.letters).lower()
    return ascii_word

files = ('whgn.jpeg', 'fwuo.png', 'ke8m.png', 'm3hn.png', '5enn.png',
         '54xe.jpeg','ea6d.jpeg','kwdg.jpeg','mkek.jpeg','nkng.jpeg',
         'w3lh.jpeg', 'teew.png')

def test_samples():
    for file in files:
        img = Image.open(file)
        print '%s is recognized as %s' %(file,ocr(img))

test_samples()
