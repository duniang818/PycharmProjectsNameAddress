import re
from collections import Counter


class StringProcess(object):
    def __init__(self):
        self.invalid_single = r"[*因为请电话联系“”'.。,，：:、;；!]*"
        self.invalid_span = r'[因为请电话（，,(].*?[)）.。]'
        self.bracket = r"[(（）)]*"
        self.bracket_content = r"[（(](.*?)[)）]"
        self.invalid_text = r"电话|联系|谢谢|大门|进去|对面|附近|rn手机号|路口往.*路"

    def invalid_characters(self, string=None):
        string = re.sub(self.invalid_single, '', string)
        bracket_content = re.findall(self.bracket_content, string)  # have multiple brackets
        b = [re.sub(self.invalid_single, '', b) for b in bracket_content]

        if len(b) != 0:
            r = re.sub(self.bracket, '', string)
        else:
            r = re.sub(self.invalid_span, '', string)
        return r

    def find_repeated(self, first, second):
        c = Counter(first) & Counter(second)
        return ''.join(c.keys())


if __name__ == '__main__':
    instring = StringProcess()
    # s = '我们发的家（顶啊花你还）能否（因为电话）'
    # r = instring.invalid_characters(s)
    # print('after string:', r)

    f = '先锋路街道二院路口往西路南桃园街202号运河花行'
    s = '广西壮族自治'
    # r = instring.find_repeated(f, s)
    r = re.sub(instring.invalid_text, '', f)
    print(r)