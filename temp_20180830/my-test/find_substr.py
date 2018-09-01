# coding=gbk
import re

a = u'放辣椒发了你好啊'
s = re.compile(u"你|我|他")
for i in s.finditer(a):
    print("i:", i.start(), i.group())
    i.start(), i.group()

for i, j in enumerate(a):
    if (j in u'你我他'):
        print(i, j)
        i, j