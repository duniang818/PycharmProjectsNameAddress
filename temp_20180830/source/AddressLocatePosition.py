# encoding: utf-8
"""
@version: 2018/9/1_V1.0
@software: PyCharm
@file: AddressLocatePosition.py
@time: 2018/9/1
@author: wym
@license: Apache Licence 
"""

import pandas as pd
import re

import requests
import logging  # 引入logging模块
import os.path
import time

class AddressProcess(object):
    def __init__(self, logger):
        self.logger = logger
        self.total_name = ['province_code', 'province', 'city_code', 'city', 'county_code', 'county', 'town_code',
                           'town', 'village_code', 'village', 'team_code', 'team', 'school_code', 'school',
                           'unit_code', 'unit', 'floor_code', 'floor', 'room_code', 'room', 'type',
                           'id', 'recipient_prov_name', 'recipient_city_name', 'recipient_county_name',
                           'recipient_name', 'recipient_mobile', 'order_create_time', 'recipient_address']
        # [village:9, team:11, school:13, unit: 15, floor: 17, room:19]
        self.my_village = pd.DataFrame(columns=['parent_code', 'name', 'code'])  # store the template data
        self.team = pd.DataFrame(columns=['parent_code', 'name', 'code'])
        self.school = pd.DataFrame(columns=['parent_code', 'name', 'code'])
        self.unit = pd.DataFrame(columns=['parent_code', 'name', 'code'])
        self.floor = pd.DataFrame(columns=['parent_code', 'name', 'code'])
        self.room = pd.DataFrame(columns=['parent_code', 'name', 'code'])
        self.source = [province, city, county, town, village, self.team, self.school, self.unit, self.floor, self.room]
        self.processed_address = {}
        self.processed_pd = pd.DataFrame(columns=self.total_name)
        self.direct_city = ['上海市', '北京市', '重庆市', '天津市']
        self.base_path = '../output_data/'
        self.my_data_path = '{}new/'.format(self.base_path)
        self.address_type = ''
        self.keywords_1 = u'省|直辖|自治|特别行政'
        self.keywords_2 = u'级市|直辖|市|直辖|自治州|盟'
        self.keywords_3 = u'市辖区|区|县|县级市|自治县|旗|自治旗|特'
        self.keywords_4 = u'镇|乡|街道马路口|路街道|街道|街|苏木|区公所|乡|苏木'
        self.keywords_5 = u'路|大道|村|社区'
        self.keywords_6 = u'弄|巷|组'
        self.keywords_7 = u'场|号楼|号库|号院|院|号|园区|园仓库|园'
        self.keywords_8 = u'幢|栋|别墅|馆大厦|所|公司|厂|局|苑'
        self.keywords_9 = u'单元|楼|层'
        self.keywords_10 = u'室|部|房'
        self.keywords = self.keywords_1 + '|' + self.keywords_2 + '|' + self.keywords_3 + '|' \
                            + self.keywords_4 + '|' + self.keywords_5 + '|' + self.keywords_6 + '|' + self.keywords_7 + '|' \
                        + self.keywords_8 + '|' + self.keywords_9 + '|' + self.keywords_10
        # self.keywords_other = self.keywords_5 + '|' + self.keywords_6 + '|' + self.keywords_7 + '|' \
        #                 + self.keywords_8 + '|' + self.keywords_9 + '|' + self.keywords_10
        # self.baidu_df = pd.DataFrame(columns=['province', 'city', 'area', 'town_code',
        #                    'town', 'village_code', 'village', 'team_code', 'team', 'school_code', 'school',
        #                    'unit_code', 'unit', 'floor_code', 'floor', 'room_code', 'room', 'type'])

        self.invalid_punctuation = r"[*\-“”^'.。,，：:、#;；!]*"
        self.invalid_span = r'[（，,(].*?[)）.。]'
        self.digialpha = r"^[0-9a-zA-Z，,.。：;:*^ ]+$"
        self.bracket = r"[(（）)]*"
        self.bracket_content = r"[（(](.*?)[)）]"
        self.invalid_text = r"电话|联系|谢谢|大门|进去|对面|附近|rn手机号|路口往.*路|收|与|交汇处|门口"
        # 行政区划区域检索
        self.area_url = 'http://api.map.baidu.com/place/v2/search?query={}&region={}&city_limit=true&scope=2&output=json&ak='
        self.circle_url = "http://api.map.baidu.com/place/v2/search?query={}&location={},{}&radius=500&output=json&ak="
        self.token = "UauOmu44P0TQEkOC8RMInMFwAFDm7dXd"

    def drop_duplicated_filed(self, value=None):
        new_value = value
        for key, val in self.processed_address.items():
            if 'code' in key or val is None:
                continue
            if val.startswith(value) or val.endswith(value) or value in val:
                value = ''
                break
            new_mask = val.replace('省', '')
            if val in value:
                new_value = value.replace(val, '')
            elif new_mask in value:
                new_value = value.replace(new_mask, '')
            value = new_value
        return value

    def drop_invalid_characters(self, string=None):
        string = re.sub(self.invalid_punctuation, '', string)
        string = re.sub(self.invalid_text, '', string)
        bracket_content = re.findall(self.bracket_content, string)  # have multiple brackets
        b = [re.sub(self.invalid_punctuation, '', b) for b in bracket_content]

        if len(b) != 0:
            r = re.sub(self.bracket, '', string)
        else:
            r = re.sub(self.invalid_span, '', string)
        return r

    def raw_process(self, df=None):
        for num, row in enumerate(df.get_values()):   # row by row
            if len(row) >= 8:
                uid = row[0]
                raw_province = row[1]
                raw_city = row[2]
                raw_county = row[3]
                recipient_name = row[4]
                recipient_mobile = row[5]
                order_create_time = row[6]
                raw_address = row[7]
                county = raw_county
                if pd.isna(raw_county):
                    county = ''
                if re.match(self.digialpha, raw_province) or re.match(self.digialpha, raw_city)\
                   or re.match(self.digialpha, county) or re.match(self.digialpha, raw_address):
                    continue
            else:
                continue
            self.logger.debug('current row:{}'.format(row))

            if not pd.isna(raw_province):
                province = self.drop_invalid_characters(raw_province)
                self.match_standard('省', province)
            if self.processed_address.get('province') in self.direct_city:
                self.processed_address['city_code'] = '{}01'.format(self.processed_address.get('province_code'))
                self.processed_address['city'] = self.processed_address.get('province')
            else:
                if not pd.isna(raw_city):
                    city = self.drop_invalid_characters(raw_city)
                    city = self.drop_duplicated_filed(city)
                    self.match_standard('市', city)
            if not pd.isna(county):
                county = self.drop_invalid_characters(county)
                county = self.drop_duplicated_filed(county)
                self.match_standard('县', county)   # 限制为第3级
            if not pd.isna(raw_address):
                address = self.drop_invalid_characters(raw_address)
                address = self.drop_duplicated_filed(address)
                new_recv_name = self.drop_invalid_characters(recipient_name)
                address = re.sub(new_recv_name, '', address)
                self.field_process(address)

            self.processed_address.update({self.total_name[20]: self.address_type})
            self.processed_address.update({self.total_name[21]: uid,
                                           self.total_name[22]: raw_province,
                                           self.total_name[23]: raw_city,
                                           self.total_name[24]: raw_county,
                                           self.total_name[25]: recipient_name,
                                           self.total_name[26]: recipient_mobile,
                                           self.total_name[27]: order_create_time,
                                           self.total_name[28]: raw_address})


            for i in range(29):
                self.processed_pd.loc[num, self.total_name[i]] = self.processed_address.get(self.total_name[i])
                if i == 9:
                    self.my_village = self.processed_pd[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]]
                #     # self.my_village.append(self.baidu_df[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]])
                elif i == 11:
                    self.team = self.processed_pd[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]]
                #     # self.team.append(
                #     #     self.baidu_df[[self.total_name[i - 3], self.total_name[i], self.total_name[i - 1]]])
                elif i == 13:
                    self.school = self.processed_pd[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]]
                #     # self.school.append(
                #     #     self.baidu_df[[self.total_name[i - 3], self.total_name[i], self.total_name[i - 1]]])
                elif i == 15:
                    self.unit = self.processed_pd[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]]
                #     # self.unit.append(
                #     #     self.baidu_df[[self.total_name[i - 3], self.total_name[i], self.total_name[i - 1]]])
                elif i == 17:
                    self.floor = self.processed_pd[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]]
                #     # self.floor.append(
                #     #     self.baidu_df[[self.total_name[i - 3], self.total_name[i], self.total_name[i - 1]]])
                elif i == 19:
                    self.room = self.processed_pd[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]]
                #     # self.room.append(
                #     #     self.baidu_df[[self.total_name[i - 3], self.total_name[i], self.total_name[i - 1]]])

            self.processed_address.clear()
            self.address_type = ''

        self.processed_pd.to_csv('{}{}_{}.csv'.format(self.base_path, 'name_address_db', time.strftime('%Y%m%d')), sep='\t', index=False, mode='a+', encoding='utf_8_sig')
        # self.baidu_df.to_csv('{}{}'.format(self.base_path, 'baidu_data.csv'), sep='\t', mode='a+', index=False)

        for file_name, data in zip(['{}{}'.format(self.my_data_path, 'my_village.csv'),
                                    '{}{}'.format(self.my_data_path, 'team.csv'),
                                    '{}{}'.format(self.my_data_path, 'school.csv'),
                                    '{}{}'.format(self.my_data_path, 'unit.csv'),
                                    '{}{}'.format(self.my_data_path, 'floor.csv'),
                                    '{}{}'.format(self.my_data_path, 'room.csv')], [self.my_village, self.team, self.school,
                                   self.unit, self.floor, self.room]):
            data = data.drop_duplicates().dropna()
            if not data.empty:
                data.to_csv(file_name, index=False, mode='a+')

    def baidu_web_api_area(self, query, region):
        area_url = self.area_url.format(query, region)
        try:
            r = requests.get(area_url + self.token)
        except ConnectionError as e:
            self.logger.error(e)
            return
        response_dict = r.json()
        # print("response:", response_dict)
        if response_dict and 'results' in response_dict.keys() and len(response_dict['results']) != 0:
            for a in response_dict['results']:
                if 'detail_info' in a.keys() and 'tag' in a['detail_info'].keys():
                    self.address_type = a['detail_info']['tag']
                return True   #搜索到结果了，可以往文件写，建立百度标准库，暂时不借用，数据量太大
            return False

    def field_process(self, values=None):
            pre_end = {}
            pre_group = {}
            remaining = values
            pattern = re.finditer(self.keywords, values)
            try:
                for i, g in enumerate(pattern):
                    pre_end[i] = g.end()
                    pre_group[i] = g.group()
                    self.logger.debug("group:{}, i:{}, span:{}".format(g.group(), i, g.span()))
                    # if g.group() == '市':
                    #     continue
                    if i == 0:  # first group
                        extract = values[:g.end()]
                        remaining = values[g.end():]
                        if extract == g.group():
                            continue
                        if len(extract) != 0:
                            extract = self.drop_invalid_characters(extract)
                        if len(extract) != 0:
                            middle = self.drop_duplicated_filed(extract)
                        if len(middle) != 0:
                            self.match_standard(g.group(), middle)
                    else:  # more than one group
                        extract = values[pre_end[i - 1]:g.end()]
                        remaining = values[g.end():]
                        if extract == g.group() and i >= 2:
                            extract = values[pre_end[i - 2]:g.end()]
                            # extract = values[:pre_end[i - 1]]
                        elif extract == g.group() and i == 1:
                            extract = values[:g.end()]
                        if len(extract) != 0:
                            extract = self.drop_invalid_characters(extract)
                        if len(extract) != 0:
                            middle = self.drop_duplicated_filed(extract)
                        if middle == g.group():
                            continue
                        if len(middle) != 0:
                            self.match_standard(g.group(), middle)
                remaining = re.sub(self.invalid_text, '', remaining)
                if len(remaining) != 0:
                    self.match_standard(match=remaining)
            except StopIteration:
                self.processed_address['level_0'] = values

    def match_standard(self, group=None, match=None):
        """
        :param group: the top 5 group
        :param match: need to match value
        :return: False: 前4级在国标里没有匹配到, 等所有的地址都分组查找完后,如何remaining值不为空,才去百度API匹配
                 True: 表示当前级在国标里匹配到了,等所有的地址都分组查找完后,如何remaining值不为空,才去百度API匹配
        """

        level = len(self.processed_address.keys())
        self.logger.debug("wait match:{}, group:{}, level:{}".format(match, group, level))
        if level == 0:
            self.query_standard(self.source[level], match, level)
        elif level == 2:
            result = self.query_standard(self.source[level//2], match, level)
            if not result:
                self.query_standard(self.source[level+1], match, level+2)  # 第3级查找
        elif level == 4:
            result = self.query_standard(self.source[level//2], match, level)
            if not result:
                result = self.adjust_changes(match)
                if not result:
                    self.query_standard(self.source[level//2], match, level)  # 用矫正后的county再去match一次
        elif level < 20:
            result = self.query_standard(self.source[level//2], match, level)
            if not result:   # 从town开始，如果没有在标准里面匹配到就调百度
                self.baidu_web_api_area(match, self.processed_address.get('city'))
        elif level >= 20:
            self.back_to_father(self.source[9], match, 18)

    def back_to_father(self, source, query, level):
        code = ''
        for i in range(level-2, -1, -2):
            parent_code = self.processed_address.get(self.total_name[i])
            if parent_code is not None:
                maxcode = source[source.code.str.find(parent_code) == 0].code.max()
                if not pd.isna(maxcode):
                    code = '{}'.format(int(maxcode) + 1)
                else:
                    if level in [0, 2, 4]:
                        pad_zero = '0'
                    else:
                        pad_zero = '00'
                    code = '{}{}1'.format(parent_code, pad_zero)
                break
        self.processed_address.update({self.total_name[level]: code,
                                       self.total_name[level + 1]: '{}{}'.
                                      format([self.processed_address.get(level+1) if self.processed_address.get(level+1) is not None else ''][0], query)})

    def query_standard(self, source, query, level):

        if level >= 8:  # begin village level
            parent_code = self.processed_address.get(self.total_name[6])
            sub_source = source[(source.parent_code.str.startswith(parent_code) == True)]
            r = sub_source[sub_source.name.str.startswith(query) == True]
        else:
            r = source[source.name.str.startswith(query) == True]
        self.logger.info('query standard:{}'.format(r))
        if not r.empty:
            if level > 2:  # 从county开始, city
                r = r[r.code.str.find(self.processed_address.get('city_code')) == 0]
            if not r.empty:
                self.processed_address.update({self.total_name[level]: r.code.get_values()[0],
                                               self.total_name[level+1]: '{}{}'.
                                              format([self.processed_address.get(level+1) if self.processed_address.get(level+1) is not None else ''][0],
                                                     r.name.get_values()[0])})
                return True
            self.back_to_father(source, query, level)
            return False
        else:
            self.back_to_father(source, query, level)
            return False

    def adjust_changes(self, county=None):
        r = changes[changes['county_name_old'] == county]
        if not r.empty:
            county = r[:1]['county_name'].get_values()[0]
            # self.match_county(county)
            yield county
        else:
            return False


def read_standard_data():
    global province
    province = pd.read_csv('province.csv', header=0, low_memory=False, dtype=str, names=['name', 'code'])
    global city
    city = pd.read_csv('city.csv', header=0, low_memory=False, dtype=str, names=['parent_code', 'name', 'code'])
    global county
    county = pd.read_csv('county.csv', header=0, low_memory=False, dtype=str, names=['parent_code', 'name', 'code'])
    global town
    town = pd.read_csv('town.csv', header=0, low_memory=False, dtype=str, names=['parent_code', 'name', 'code'])

    global changes
    c = pd.read_csv('name_change.csv', low_memory=False)
    changes = c.filter(items=['county_name_old', 'county_name'])

    output_path = '../output_data/'
    try:
        global village
        village = pd.read_csv('{}{}'.format(output_path, 'village.csv'), header=0, low_memory=False, dtype=str, names=['parent_code', 'name', 'code'])
    except FileNotFoundError as e:  # 如果合并没有成功则只读取source目录下国标village
        village = pd.read_csv('village.csv', header=0, low_memory=False, dtype=str, names=['parent_code', 'name', 'code'])

    try:
        global school
        school = pd.read_csv('{}{}'.format(output_path, 'school.csv'), names=['parent_code', 'name', 'code'], sep='\t',  low_memory=False,
                             dtype=str).drop_duplicates()
    except Exception as e:
        school = None
    try:
        global team
        team = pd.read_csv('{}{}'.format(output_path, 'team.csv'), names=['parent_code', 'name', 'code'], sep='\t',
                                   low_memory=False, dtype=str).drop_duplicates()
    except Exception as e:
        team = None

    try:
        global unit
        unit = pd.read_csv('{}{}'.format(output_path, 'unit.csv'), names=['parent_code', 'name', 'code'], sep='\t',
                               low_memory=False, dtype=str).drop_duplicates()
    except Exception as e:
        unit = None

    try:
        global floor
        floor = pd.read_csv('{}{}'.format(output_path, 'floor.csv'), names=['parent_code', 'name', 'code'], sep='\t',
                            low_memory=False, dtype=str).drop_duplicates()
    except Exception as e:
        floor = None

    try:
        global room
        room = pd.read_csv('{}{}'.format(output_path, 'room.csv'), names=['parent_code', 'name', 'code'], sep='\t',
                           low_memory=False, dtype=str).drop_duplicates()
    except Exception as e:
        room = None


def read_file_multipool(logger):
    pdf = pd.read_csv('../input_data/test_file_2.csv', sep='\t', header=0, iterator=True,
                      names=['id', 'recipient_prov_name', 'recipient_city_name',
                             'recipient_county_name', 'recipient_name', 'recipient_mobile', 'order_create_time',
                             'recipient_address'])
    loop = True
    chunk_size = 1000
    read_standard_data()
    ap = AddressProcess(logger)
    while loop:
        try:
            chunk = pdf.get_chunk(chunk_size)
            if chunk is not None:
                ap.raw_process(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")


def log_to_file():

    # 第一步，创建一个logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Log等级总开关
    # 第二步，创建一个handler，用于写入日志文件
    # rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
    rq = time.strftime('%Y%m%d%H', time.localtime(time.time()))
    log_path = os.path.dirname(os.getcwd()) + '/logs/'
    log_name = log_path + rq + '.log'
    fh = logging.FileHandler(log_name, mode='w')
    # fh.setLevel(logging.info())  # 输出到file的log等级的开关
    # 第三步，定义handler的输出格式
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh.setFormatter(formatter)
    # 第四步，将logger添加到handler里面
    logger.addHandler(fh)
    # 日志
    # logger.debug('this is a logger debug message')
    # logger.info('this is a logger info message')
    # logger.warning('this is a logger warning message')
    # logger.error('this is a logger error message')
    # logger.critical('this is a logger critical message')
    return logger


def rewrite_my_standard_data():
    try:
        team = pd.read_csv('{}{}'.format(my_data_path, 'team.csv'), header=0, na_filter=False, low_memory=False,
                              names=['parent_code', 'name', 'code'], dtype=str).drop_duplicates()
        team.to_csv('../output_data/team.csv', index=False, mode='a+')
        school = pd.read_csv('{}{}'.format(my_data_path, 'school.csv'), header=0, na_filter=False, low_memory=False,
                           names=['parent_code', 'name', 'code'], dtype=str).drop_duplicates()
        school.to_csv('../output_data/school.csv', index=False)

        unit = pd.read_csv('{}{}'.format(my_data_path, 'unit.csv'), header=0, na_filter=False, low_memory=False,
                             names=['parent_code', 'name', 'code'], dtype=str).drop_duplicates()
        unit.to_csv('../output_data/unit.csv', index=False)

        floor = pd.read_csv('{}{}'.format(my_data_path, 'floor.csv'), header=0, na_filter=False, low_memory=False,
                             names=['parent_code', 'name', 'code'], dtype=str).drop_duplicates()
        floor.to_csv('../output_data/floor.csv', index=False)

        room = pd.read_csv('{}{}'.format(my_data_path, 'room.csv'), header=0, na_filter=False, low_memory=False,
                             names=['parent_code', 'name', 'code'], dtype=str).drop_duplicates()
        room.to_csv('../output_data/room.csv', index=False)
    except FileNotFoundError as e:
        print(e)


def df_join():
    try:
        village = pd.read_csv('{}{}'.format('../source/', 'village.csv'), header=0, na_filter=False, low_memory=False,
                              names=['parent_code', 'name', 'code'], dtype=str)

        my_village = pd.read_csv('{}{}'.format(my_data_path, 'my_village.csv'),
                                 names=['parent_code', 'name', 'code'],
                                 low_memory=False, header=0,
                                 dtype=str).drop_duplicates()
        village = village.append(my_village).drop_duplicates()
        village.to_csv('../output_data/village.csv', index=False)
    except FileNotFoundError as e:
        print(e)

base_path = '../output_data/'
my_data_path = '{}new/'.format(base_path)

if __name__ == '__main__':
    rewrite_my_standard_data()
    df_join()
    logger = log_to_file()
    read_file_multipool(logger)

