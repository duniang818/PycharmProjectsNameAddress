# encoding: utf-8
"""
@version: python 3.6
@author:  wym
@license: Apache Licence 
@time: 2018/8/12
"""
from collections import Counter

import pandas as pd, numpy as np
import pymysql, re
from multiprocessing import Pool



def pd_mysql():
    conn = pymysql.connect(host="localhost", user="root", passwd="yto2018", db='area_code', charset="utf8",
                           cursorclass=pymysql.cursors.DictCursor)

    # sql = 'select province, province_code,city,city_code,county,county_code,town,town_code from area_code'
    sql = 'select parent_code, village_name, village_code from village_2'
    # sql = "select p.province_name as province, c.province_code, " \
    #       "if(c.city_name='市辖区', p.province_name, c.city_name) as city, c.city_code, " \
    #       " co.county_name as county, co.county_code,if(t.town_name like '%办事处', " \
    #       "replace(t.town_name, '办事处', ''), t.town_name) as town,  t.town_code from province p left join city c " \
    #       "on p.province_code=c.province_code left join county co on co.city_code=c.city_code " \
    #       "left join town t on t.county_code=co.county_code " \
    #       "left join village v on v.town_code = t.town_code"
    df = pd.read_sql(sql, con=conn).drop_duplicates()
    # print(df.head())

    df.to_csv("../source/area_village.csv", index=False)
    conn.close()


def raw_process(narray=None):
    for row in narray:
        if len(row) == 1:
            processed_address = row_iter_process(row[0])
        else:
            print("error address:".format(row))

def row_iter_process(values=None):
    pre_end = {}
    pre_group = {}
    j = 0
    last = 'level_0'
    remaining = values
    processed_address = {}
    keywords_1 = u'省|直辖市|自治区|特别行政区'
    keywords_2 = u'地级市|直辖区|市|直辖县|自治州|地区|盟'
    keywords_3 = u'市辖区|区|县|县级市|自治县|旗|自治旗|特区|林区'
    keywords_4 = u'街道|街|道|镇|乡|苏木|区公所|民族乡|民族苏木'
    keywords = keywords_1 + '|' + keywords_2 + '|' + keywords_3 + '|' \
               + keywords_4
    pattern = re.finditer(keywords, values)
    print("current:", values)
    try:
        for i, g in enumerate(pattern):
            pre_end[i] = g.end()
            pre_group[i] = g.group()
            # print("p:{}, group:{}".format(g, g.group()))
            if i == 0:
                extract = values[:g.end()]
                if extract in processed_address.values():
                    continue
            elif i == 1:
                extract = values[pre_end[i - 1]:g.end()]
                remaining = values[g.end():]
                if extract in processed_address.values() or extract == g.group() or pre_group[i - 1] == g.group():
                    # print("111ex:{}, group:{}, p:{}".format(extract, g.group(), pro0cessed_address))
                    continue
                # if pre_group[i - 1] == g.group() or pre_end[i - 1] == (g.end() - len(g.group())):
                #     extract = values[:g.end()]  # 挨着的两个关键字一样的话，归并在一起 # 判断只有一个关键字的情况，如9号楼，不能拆分成9号，楼
            else:
                extract = values[pre_end[i - 1]:g.end()]
                remaining = values[g.end():]
                if extract in processed_address.values() or extract == g.group() or pre_group[i - 1] == g.group():
                    # print("222ex:{}, group:{}, p:{}".format(extract, g.group(), processed_address))
                    continue
                # if pre_group[i - 1] == g.group() or pre_end[i - 1] == (g.end() - len(g.group())):
                #     extract = values[pre_end[i - 2]:g.end()]  # 挨着的两个关键字一样的话，认为是同一级, 如高新区 西区
            if extract in processed_address.values():  # 如果拼接完了和前面的一样的话，也需要抛弃后面的，等于去重，不过都是全字匹配
                continue
            remaining = values[g.end():]
            print("ex:{}, re:{}".format(extract, remaining))
            if j > 0:
                if pre_group[j - 1] == g.group() or pre_end[j - 1] == (g.end() - len(g.group())):
                    key = 'level_{}'.format(j - 1)
                    processed_address[key] = extract
                    last = 'level_{}'.format(j)
                else:
                    key = 'level_{}'.format(j)
                    processed_address[key] = extract
                    last = 'level_{}'.format(j + 1)
            else:
                key = 'level_{}'.format(j)
                processed_address[key] = extract
                last = 'level_{}'.format(j + 1)
            j += 1
        if len(remaining) != 0:
            processed_address[last] = remaining
        return processed_address
    except StopIteration:
        processed_address['level_0'] = values
        return processed_address

def pd_csv():
    pdf = pd.read_csv('test_file.csv', iterator=True, header=None, na_filter=False)
    # pdf = pd.read_csv('000000_0.csv', sep='\001', chunksize=10000)
    invalid_string = r"\001[a-zA-Z0-9]*"
    # pdf = pd.read_csv('test_file.csv', header=None).replace(to_replace=invalid_string, value='', regex=True)
    loop = True
    chunksize = 1
    chunks = []
    while loop:
        try:
            chunk = pdf.get_chunk(chunksize).replace(to_replace=invalid_string, value='', regex=True)
            chunks.append(chunk.get_values())
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    size = len(chunks)
    p = Pool(size)
    for bulk in chunks:
        # print('bulk:', bulk, type(bulk), len(bulk))
        p.apply_async(raw_process(bulk))
    p.close()
    p.join()

def find_repeated(first, second):
    c = Counter(''.join(first)) & Counter(''.join(second))
    return ''.join(c.keys())

def read_standard_data():

        standard = pd.read_csv('area_code_2017.csv', na_filter=False, low_memory=False)
        standard[['province_code', 'city_code', 'county_code', 'town_code']] = standard[['province_code', 'city_code', 'county_code', 'town_code']].astype(str)

        # s = find_repeated('北京', standard.province_code)
        # print(s)
        # print('standard:', standard.head())
        standard.to_csv('area_code.csv', index=False, encoding='utf-8')

        # print('where:', standard[standard['county'] == '万源市']['city'].get_values()[0])
        # c = standard.loc[standard['county'] == '万源市']
        # print(c)
if __name__ == '__main__':
    # read_standard_data()
    # pd_mysql()
    # d = [['32', '33', '12'], ['30', '33', '12']]
    # df = pd.DataFrame(columns=['a', 'b', 'c'], dtype=str)
    # df = df.append(d)
    # # df['a'].max()
    # # df.loc[1, 'a'] = '2'
    # # df.loc[1, 'b'] = '3'
    # # df.a = '1'
    # print(df)
    # print(int(df[0].max()) + 1)
    df = pd.read_csv('../output_data/floor.csv', names=['parent_code', 'name', 'code'], header=None,  sep='\t')
    # print(df.get('name').str.startswith('1单元'), df.get('name').str.find('1单元'))
    # print(df.where(df.get('name').str.find('单元') != -1, df, axis=0))
    # print(df.query(df.name.str.find('单元')!=-1))
    print(df[df.name.str.find('1单元') == 0])