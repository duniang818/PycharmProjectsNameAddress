# encoding: utf-8
"""
@version: python 3.6
@author:  wym
@license: Apache Licence 
@time: 2018/8/22
"""

import hashlib
import requests




if __name__ == '__main__':
    # url = 'https://maps.googleapis.com/maps/api/geocode/json'
    # params = {'sensor': 'false', 'address': '陕西省西安市'}
    # r = requests.get(url, params=params)
    # results = r.json()['results']
    # location = results[0]['geometry']['location']
    # location['lat'], location['lng']
    #
    # g = geocoder.google('Mountain View, CA')
    # data = "丈八沟街道"
    # m = hashlib.md5(data.encode("gb2312"))
    # print(m.hexdigest())
    # print(data.encode('gbk'))

    import uuid

    """
    uuid是128位的全局唯一标识符（univeral unique identifier），通常用32位的一个字符串的形式来表现。有时也称guid(global unique identifier)。python中自带了uuid模块来进行uuid的生成和管理工作。（具体从哪个版本开始有的不清楚。。）
　　python中的uuid模块基于信息如MAC地址、时间戳、命名空间、随机数、伪随机数来uuid。具体方法有如下几个：　　
　　uuid.uuid1()　　基于MAC地址，时间戳，随机数来生成唯一的uuid，可以保证全球范围内的唯一性。
　　uuid.uuid2()　　算法与uuid1相同，不同的是把时间戳的前4位置换为POSIX的UID。不过需要注意的是python中没有基于DCE的算法，所以python的uuid模块中没有uuid2这个方法。
　　uuid.uuid3(namespace,name)　　通过计算一个命名空间和名字的md5散列值来给出一个uuid，所以可以保证命名空间中的不同名字具有不同的uuid，但是相同的名字就是相同的uuid了。【感谢评论区大佬指出】namespace并不是一个自己手动指定的字符串或其他量，而是在uuid模块中本身给出的一些值。比如uuid.NAMESPACE_DNS，uuid.NAMESPACE_OID，uuid.NAMESPACE_OID这些值。这些值本身也是UUID对象，根据一定的规则计算得出。
　　uuid.uuid4()　　通过伪随机数得到uuid，是有一定概率重复的
　　uuid.uuid5(namespace,name)　　和uuid3基本相同，只不过采用的散列算法是sha1
　　一般而言，在对uuid的需求不是很复杂的时候，uuid1方法就已经够用了，使用方法如下：
    """
    name = '丈八沟'
    # namespace = 'test_namespace'
    namespace = uuid.NAMESPACE_URL

    print(uuid.uuid1())
    print(uuid.uuid3(uuid.NAMESPACE_DNS, name))
    print(uuid.uuid4())
    print(uuid.uuid5(namespace, name))

    """
    c83061ec-a5fc-11e8-a4f6-e489bd143495
c8a964ad-2b0f-3403-b1f2-d8a3d1d26170
d70c008b-7c90-4048-a4d4-41d9f71ca469
0794b349-c8fa-51c6-b717-a7ed64619366
    """