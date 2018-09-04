# encoding: utf-8
"""
@version: python 3.6
@author:  YTO
@license: Apache Licence 
@time: 2018/8/22
"""

def mydiff():
    import difflib
    query_str = '榕华街道'

    s1 = '榕华街道'

    s2 = '榕东街道'

    s3 = '广州市检查院'
    list = [s1, s2, s3]
    # for s in list:
    #     print(difflib.SequenceMatcher(None, query_str, s).quick_ratio())
    print(difflib.SequenceMatcher(None, query_str, s1).quick_ratio())
    # import keyword
    # print(difflib.get_close_matches(query_str, keyword.kwlist))
    print(difflib.get_close_matches(query_str, list, 1))
    #
    # print(difflib.SequenceMatcher(None, query_str, s2).quick_ratio())
    #
    # print(difflib.SequenceMatcher(None, query_str, s3).quick_ratio())

    # def find_repeated(self, first, second):
    #     for val in second:
    #         if type(val) != 'str':
    #             continue
    #         second = second + val
    #     c = Counter(first) & Counter(second)
    #     return ''.join(c.keys())

    # with open('processed.txt', 'a+', newline='', ) as file:
    #     # fieldnames = self.level_name
    #     # writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #     #
    #     # # writer.writeheader()
    #     # writer.writerow(self.processed_address.values())
    #     # js = json.dumps(self.processed_address)
    #     seq = '\t'
    #     file.writelines(seq.join(self.processed_address.values()))
    # file.close()

def mysimilar():
    import difflib
    import Levenshtein as ls

    str1 = "我的骨骼雪白 也长不出青稞"

    str2 = "雪的日子 我只想到雪中去si"

    # 1. difflib

    seq = difflib.SequenceMatcher(None, str1, str2)

    ratio = seq.ratio()

    print('difflib similarity1: ', ratio)

    # difflib 去掉列表中不需要比较的字符

    seq = difflib.SequenceMatcher(lambda x: x in ' 我的雪', str1, str2)

    ratio = seq.ratio()

    print('difflib similarity2: ', ratio)


    # 2. hamming距离，str1和str2长度必须一致，描述两个等长字串之间对应位置上不同字符的个数

    # sim = ls.hamming(str1, str2)

    # print 'hamming similarity: ', sim

    # 3. 编辑距离，描述由一个字串转化成另一个字串最少的操作次数，在其中的操作包括 插入、删除、替换

    sim = ls.distance(str1, str2)

    print('ls similarity: ', sim)


    # 4.计算莱文斯坦比

    sim = ls.ratio(str1, str2)

    print('ls.ratio similarity: ', sim)


    # 5.计算jaro距离

    sim = ls.jaro(str1, str2)

    print('ls.jaro similarity: ', sim)


    # 6. Jaro–Winkler距离

    sim = ls.jaro_winkler(str1, str2)

    print('ls.jaro_winkler similarity: ', sim)


if __name__ == '__main__':
    # mydiff()
    for i in range(4, -1, -1):
        print(i)