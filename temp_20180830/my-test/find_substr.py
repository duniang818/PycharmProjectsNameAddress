# coding=gbk
import re

a = u'������������ð�'
s = re.compile(u"��|��|��")
for i in s.finditer(a):
    print("i:", i.start(), i.group())
    i.start(), i.group()

for i, j in enumerate(a):
    if (j in u'������'):
        print(i, j)
        i, j