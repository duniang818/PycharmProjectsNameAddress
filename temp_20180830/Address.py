# encoding: utf-8
"""
@version: python 3.6
@author:  wym
@license: Apache Licence 
@time: 2018/8/12
"""
import requests
import pandas as pd
import re

import numpy as np
# from tabulate import tabulate

class AddressProcess(object):
    def __init__(self):
        # self.invalid_punctuation = invalid_punctuation = r"[*(（）)【“”'.。,，：:、;；】!]*"
        self.invalid_punctuation = r"[*\-“”^'.。,，：:、#;；!]*"
        self.invalid_span = r'[（，,(].*?[)）.。]'
        self.digialpha = r"^[0-9a-zA-Z，,.。：;:*^ ]+$"
        self.bracket = r"[(（）)]*"
        self.bracket_content = r"[（(](.*?)[)）]"
        self.invalid_text = r"电话|联系|谢谢|大门|进去|对面|附近"

        self.keywords_1 = u'省|直辖|自治|特别行政'
        self.keywords_2 = u'级市|直辖|市|直辖|自治州|盟'
        self.keywords_3 = u'市辖区|区|县|县级市|自治县|旗|自治旗|特'
        self.keywords_4 = u'镇|乡|路街道|街道|街道马路口|街|苏木|区公所|乡|苏木'
        self.keywords_5 = u'路|大道|村|社区'
        self.keywords_6 = u'弄|巷|组'
        self.keywords_7 = u'场|号|院|园'
        self.keywords_8 = u'幢|栋|别墅|大厦|所|公司|厂|局|苑'
        self.keywords_9 = u'单元|楼|层'
        self.keywords_10 = u'室|部|房'
        # self.keywords = self.keywords_1 + '|' + self.keywords_2 + '|' + self.keywords_3 + '|' \
        #                     + self.keywords_4 + '|' + self.keywords_5 + '|' + self.keywords_6 + '|' + self.keywords_7 + '|' \
        #                 + self.keywords_8 + '|' + self.keywords_9 + '|' + self.keywords_10
        self.total_keywords = [self.keywords_1, self.keywords_2, self.keywords_3, self.keywords_4, self.keywords_5,
                               self.keywords_6, self.keywords_7, self.keywords_8, self.keywords_9, self.keywords_10]
        self.keywords_other = self.keywords_5 + '|' + self.keywords_6 + '|' + self.keywords_7 + '|' \
                        + self.keywords_8 + '|' + self.keywords_9 + '|' + self.keywords_10

        self.total_name = ['province_code', 'province', 'city_code', 'city', 'county_code', 'county', 'town_code',
                           'town', 'village_code', 'village', 'team_code', 'team', 'school_code', 'school',
                           'unit_code', 'unit', 'floor_code', 'floor', 'room_code', 'room', 'type'
            , 'id', 'recipient_prov_name', 'recipient_city_name', 'recipient_county_name', 'recipient_name', 'recipient_mobile', 'order_create_time', 'recipient_address']
        # [village:9, team:11, school:13, unit: 15, floor: 17, room:19]
        self.village = pd.DataFrame(columns=['parent_code', 'name', 'code'])  # store the template data
        self.team = pd.DataFrame(columns=['parent_code', 'name', 'code'])
        self.school = pd.DataFrame(columns=['parent_code', 'name', 'code'])
        self.unit = pd.DataFrame(columns=['parent_code', 'name', 'code'])
        self.floor = pd.DataFrame(columns=['parent_code', 'name', 'code'])
        self.room = pd.DataFrame(columns=['parent_code', 'name', 'code'])

        self.baidu_df = pd.DataFrame(columns=['province', 'city', 'area', 'town_code',
                           'town', 'village_code', 'village', 'team_code', 'team', 'school_code', 'school',
                           'unit_code', 'unit', 'floor_code', 'floor', 'room_code', 'room', 'type'])
        self.processed_address = {}
        self.error_address = []
        self.processed_pd = pd.DataFrame(columns=self.total_name)
        self.base_path = '../output_data/'
        # 行政区划区域检索
        self.areurl = 'http://api.map.baidu.com/place/v2/search?query={}&region={}&city_limit=true&scope=2&output=json&ak='
        self.circle_url = "http://api.map.baidu.com/place/v2/search?query={}&location={},{}&radius=500&output=json&ak="
        self.token = "UauOmu44P0TQEkOC8RMInMFwAFDm7dXd"

    def drop_duplicated_filed(self, value=None):
        new_value = value
        for key, val in self.processed_address.items():
            if 'code' in key or val is None:
                continue
            new_mask = val.replace('省', '')
            if val in value:
                new_value = value.replace(val, '')
            elif new_mask in value:
                new_value = value.replace(new_mask, '')
                # print('drop duplicated:', new_value)
            value = new_value
        return value

    def drop_invalid_characters(self, string=None):
        string = re.sub(self.invalid_punctuation, '', string)
        string = re.sub(self.invalid_text, '', string)
        bracket_content = re.findall(self.bracket_content, string)  # have multiple brackets
        b = [re.sub(self.invalid_punctuation, '', b) for b in bracket_content]

        if len(b) != 0:
            r = re.sub(self.bracket, '', string)
            # r = re.sub(self.invalid_text, '', string)
        else:
            r = re.sub(self.invalid_span, '', string)
        return r

    def raw_process(self, df=None):
        for num, row in enumerate(df.get_values()):   # row by row
            if len(row) >= 8:
                uid = row[0]
                province = row[1]
                city = row[2]
                county = row[3]
                recipient_name = row[4]
                recipient_mobile = row[5]
                order_create_time = row[6]
                raw_address = row[7]
                # if pd.isna(raw_address):
                #     continue
                if pd.isna(county):
                    county = ''
                if re.match(self.digialpha, province) or re.match(self.digialpha, city)\
                   or re.match(self.digialpha, county) or re.match(self.digialpha, raw_address):
                    continue
            else:
                continue
            print('current row:', province, city, county, raw_address)

            if not pd.isna(province):
                province = self.drop_invalid_characters(province)
                self.match_standard('省', province)
            if not pd.isna(city):
                city = self.drop_invalid_characters(city)
                city = self.drop_duplicated_filed(city)
                self.match_standard('市', city)
            if not pd.isna(county):
                county = self.drop_invalid_characters(county)
                county = self.drop_duplicated_filed(county)
                self.match_standard('县', county)   # 限制为第3级
            if not pd.isna(raw_address):
                address = self.drop_invalid_characters(raw_address)
                address = self.drop_duplicated_filed(address)
                self.row_iter_process(address)

            self.processed_address.update({self.total_name[21]: uid, self.total_name[22]: province, self.total_name[23]: city,
                                           self.total_name[24]: county, self.total_name[25]: recipient_name,
                                           self.total_name[26]: recipient_mobile, self.total_name[27]: order_create_time,
                                           self.total_name[28]: raw_address})


            for i in range(29):
                self.processed_pd.loc[num, self.total_name[i]] = self.processed_address.get(self.total_name[i])
                if i == 9:
                    self.village = self.processed_pd[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]]
                    self.village.append(self.baidu_df[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]])
                elif i == 11:
                    self.team = self.processed_pd[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]]
                    self.team.append(
                        self.baidu_df[[self.total_name[i - 3], self.total_name[i], self.total_name[i - 1]]])
                elif i == 13:
                    self.school = self.processed_pd[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]]
                    self.school.append(
                        self.baidu_df[[self.total_name[i - 3], self.total_name[i], self.total_name[i - 1]]])
                elif i == 15:
                    self.unit = self.processed_pd[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]]
                    self.unit.append(
                        self.baidu_df[[self.total_name[i - 3], self.total_name[i], self.total_name[i - 1]]])
                elif i == 17:
                    self.floor = self.processed_pd[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]]
                    self.floor.append(
                        self.baidu_df[[self.total_name[i - 3], self.total_name[i], self.total_name[i - 1]]])
                elif i == 19:
                    self.room = self.processed_pd[[self.total_name[i-3], self.total_name[i], self.total_name[i-1]]]
                    self.room.append(
                        self.baidu_df[[self.total_name[i - 3], self.total_name[i], self.total_name[i - 1]]])
            # for (level, i) in zip([self.village, self.team, self.school, self.unit, self.floor, self.room],
            #                       [8, 10, 12, 14, 16, 18]):
            #     self.customer_encode(level=level, index=i, basenum=num)
            self.processed_address.clear()

        self.processed_pd.to_csv('{}{}'.format(self.base_path, 'name_address_db.csv'), sep='\t', index=False, mode='a+', encoding='utf_8_sig')
        self.baidu_df.to_csv('{}{}'.format(self.base_path, 'baidu_data.csv'), sep='\t', mode='a+', index=False)

        for file_name, data in zip(['{}{}'.format(self.base_path, 'village.csv'),
                                    '{}{}'.format(self.base_path, 'team.csv'),
                                    '{}{}'.format(self.base_path, 'school.csv'),
                                    '{}{}'.format(self.base_path, 'unit.csv'),
                                    '{}{}'.format(self.base_path, 'floor.csv'),
                                    '{}{}'.format(self.base_path, 'room.csv')], [self.village, self.team, self.school,
                                   self.unit, self.floor, self.room]):
            # print('data:', data.drop_duplicates().dropna())
            data = data.drop_duplicates().dropna()
            if not data.empty:
                data.to_csv(file_name, sep='\t', index=False, mode='a+', header=None)

    def row_iter_process(self, values=None):
        pre_end = {}
        pre_group = {}
        remaining = values
        pattern = re.finditer(self.keywords, values)
        # print("current:", values)
        try:
            for i, g in enumerate(pattern):
                pre_end[i] = g.end()
                pre_group[i] = g.group()
                print("group:{}, i:{}, span:{}".format(g.group(), i, g.span()))
                if g.group() == '市':
                    continue
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
                        # extract = values[:pre_end[i-1]]
                    if len(extract) != 0:
                        extract = self.drop_invalid_characters(extract)
                    if len(extract) != 0:
                        middle = self.drop_duplicated_filed(extract)
                    if middle == g.group():
                        continue
                    if len(middle) != 0:
                        self.match_standard(g.group(), middle)

            # print('remaining:{}'.format(remaining))
            remaining = re.sub(self.invalid_text, '', remaining)
            if len(remaining) != 0:
                self.match_standard('室', remaining)
                # for key in ['s']:
                #     if key
                if ('city' and 'school' and 'county') in self.processed_address.keys():
                    self.search_baidu(remaining, self.processed_address['city']
                                                  + self.processed_address['county'])
                elif 'city' in self.processed_address.keys():
                    self.search_baidu(remaining, self.processed_address['city'])
            else:
                # if self.processed_address.get(self.total_name[3]) and self.processed_address.get(self.total_name[5]):
                if ('city' and 'county' and 'village') in self.processed_address.keys():
                    self.search_baidu(self.processed_address['village'], self.processed_address['city'] + self.processed_address['county'])
                elif ('city' and 'county' and 'unit') in self.processed_address.keys():
                    self.search_baidu(self.processed_address['unit'], self.processed_address['city'] + self.processed_address['county'])
        except StopIteration:
            self.processed_address['level_0'] = values

    def match_custom_level(self, level_name, level_standard, match):
        key_code = level_name + '_code'
        if len(match) != 0:
            # print(level_name, level_standard, level_standard.get('parent_code'), self.processed_pd.get(parent_code))
            if level_standard is not None and not level_standard.empty:  #file exists, have content, have key
                r = self.select_similar(match, level_standard.get('name').get_values())
                if r is not None:
                    code = level_standard[level_standard.get('name') == r]['code'].get_values()[0]
                    self.processed_address[key_code] = code
                    if self.processed_address.get(level_name):
                        self.processed_address[level_name] = self.processed_address[level_name] + r
                    else:
                        self.processed_address[level_name] = r
                    return True
                else:
                    #  first match the current name, 但是可能有父级
                    if level_name == 'village':
                        if self.processed_address.get('town'):
                            town_code = self.processed_address.get('town_code')
                            r = level_standard[level_standard.get('parent_code') == town_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(town_code)
                        elif self.processed_address.get('county'):
                            print('exists')
                        else:
                            print('test case')
                    elif level_name == 'team':
                        if self.processed_address.get('village'):
                            village_code = self.processed_address.get('village_code')
                            r = level_standard[level_standard.get('parent_code') == village_code]
                            if not r.empty:
                                num = r.get('code').max()
                                # print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(village_code)
                        elif self.processed_address.get('town'):
                            town_code = self.processed_address.get('town_code')

                            r = level_standard[level_standard.get('parent_code') == town_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(town_code)
                        elif self.processed_address.get('county'):
                            print('exists')
                        else:
                            print('test case 2')
                    elif level_name == 'school':
                        if self.processed_address.get('team'):
                            team_code = self.processed_address.get('team_code')
                            r = level_standard[level_standard.get('parent_code') == team_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(team_code)
                        elif self.processed_address.get('village'):
                            village_code = self.processed_address.get('village_code')
                            r = level_standard[level_standard.get('parent_code') == village_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(village_code)
                        elif self.processed_address.get('town'):
                            town_code = self.processed_address.get('town_code')

                            r = level_standard[level_standard.get('parent_code') == town_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(town_code)
                        elif self.processed_address.get('county'):
                            print('exists')
                        else:
                            print('test case 2')
                    elif level_name == 'unit':
                        if self.processed_address.get('school'):
                            school_code = self.processed_address.get('school_code')
                            r = level_standard[level_standard.get('parent_code') == school_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(school_code)
                        elif self.processed_address.get('team'):
                            team_code = self.processed_address.get('team_code')
                            r = level_standard[level_standard.get('parent_code') == team_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(team_code)
                        elif self.processed_address.get('village'):
                            village_code = self.processed_address.get('village_code')
                            r = level_standard[level_standard.get('parent_code') == village_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(village_code)
                        elif self.processed_address.get('town'):
                            town_code = self.processed_address.get('town_code')

                            r = level_standard[level_standard.get('parent_code') == town_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(town_code)
                        elif self.processed_address.get('county'):
                            print('exists')
                        else:
                            print('test case 2')
                    elif level_name == 'floor':
                        if self.processed_address.get('unit'):
                            unit_code = self.processed_address.get('unit_code')
                            r = level_standard[level_standard.get('parent_code') == unit_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(unit_code)
                        elif self.processed_address.get('school'):
                            school_code = self.processed_address.get('school_code')
                            r = level_standard[level_standard.get('parent_code') == school_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(school_code)
                        elif self.processed_address.get('team'):
                            team_code = self.processed_address.get('team_code')
                            r = level_standard[level_standard.get('parent_code') == team_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(team_code)
                        if self.processed_address.get('village'):
                            village_code = self.processed_address.get('village_code')
                            r = level_standard[level_standard.get('parent_code') == village_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(village_code)
                        elif self.processed_address.get('town'):
                            town_code = self.processed_address.get('town_code')

                            r = level_standard[level_standard.get('parent_code') == town_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(town_code)
                        elif self.processed_address.get('county'):
                            print('exists')
                        else:
                            print('test case 2')
                    elif level_name == 'room':
                        if self.processed_address.get('floor'):
                            floor_code = self.processed_address.get('floor_code')
                            r = level_standard[level_standard.get('parent_code') == floor_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(floor_code)
                        elif self.processed_address.get('unit'):
                            unit_code = self.processed_address.get('unit_code')
                            r = level_standard[level_standard.get('parent_code') == unit_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(unit_code)
                        elif self.processed_address.get('school'):
                            school_code = self.processed_address.get('school_code')
                            r = level_standard[level_standard.get('parent_code') == school_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(school_code)
                        elif self.processed_address.get('team'):
                            team_code = self.processed_address.get('team_code')
                            r = level_standard[level_standard.get('parent_code') == team_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(team_code)
                        elif self.processed_address.get('village'):
                            village_code = self.processed_address.get('village_code')
                            r = level_standard[level_standard.get('parent_code') == village_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(village_code)
                        elif self.processed_address.get('town'):
                            town_code = self.processed_address.get('town_code')

                            r = level_standard[level_standard.get('parent_code') == town_code]
                            if not r.empty:
                                num = r.get('code').max()
                                print('num:', num)
                                self.processed_address[key_code] = str(int(num) + 1)  # 此类下非第一个
                            else:
                                # 如果在文件中没有找到父级的编码，则证明是第一个此类别下的编码，因此直接在父类后拼接上0001
                                self.processed_address[key_code] = '{}0001'.format(town_code)
                        elif self.processed_address.get('county'):
                            print('exists')
                        else:
                            print('test case 2')
                    if self.processed_address.get(level_name):
                        self.processed_address[level_name] = self.processed_address[level_name] + match
                    else:
                        self.processed_address[level_name] = match
                    return False
            else:
                print('first time match {}'.format(level_name))
                if level_name == 'village':
                    if self.processed_address.get('town'):
                        town_code = self.processed_address.get('town_code')
                        self.processed_address[key_code] = '{}0001'.format(town_code)
                    elif self.processed_address.get('county'):
                        county_code = self.processed_address.get('county_code')
                        self.processed_address[key_code] = '{}0000001'.format(county_code)
                    else:
                        self.processed_address[key_code] = '{}000000001'.format(self.processed_address.get('city_code'))
                elif level_name == 'team':
                    if self.processed_address.get('village'):
                        village_code = self.processed_address.get('village_code')
                        self.processed_address[key_code] = '{}0001'.format(village_code)
                    elif self.processed_address.get('town'):
                        town_code = self.processed_address.get('town_code')
                        self.processed_address[key_code] = '{}00000001'.format(town_code)
                    elif self.processed_address.get('county'):
                        county_code = self.processed_address.get('county_code')
                        self.processed_address[key_code] = '{}000000000001'.format(county_code)
                    else:
                        self.processed_address[key_code] = '{}0000000000000001'.format(self.processed_address.get('city_code'))
                elif level_name == 'school':
                    if self.processed_address.get('team'):
                        team_code = self.processed_address.get('team_code')
                        self.processed_address[key_code] = '{}0001'.format(team_code)
                    elif self.processed_address.get('village'):
                        village_code = self.processed_address.get('village_code')
                        self.processed_address[key_code] = '{}00000001'.format(village_code)
                    elif self.processed_address.get('town'):
                        town_code = self.processed_address.get('town_code')
                        self.processed_address[key_code] = '{}000000000001'.format(town_code)
                    elif self.processed_address.get('county'):
                        county_code = self.processed_address.get('county_code')
                        self.processed_address[key_code] = '{}0000000000000001'.format(county_code)
                    else:
                        self.processed_address[key_code] = '{}00000000000000000001'.format(self.processed_address.get('city_code'))
                elif level_name == 'unit':
                    if self.processed_address.get('school'):
                        school_code = self.processed_address.get('school_code')
                        self.processed_address[key_code] = '{}0001'.format(school_code)
                    elif self.processed_address.get('team'):
                        team_code = self.processed_address.get('team_code')
                        self.processed_address[key_code] = '{}00000001'.format(team_code)
                    elif self.processed_address.get('village'):
                        village_code = self.processed_address.get('village_code')
                        self.processed_address[key_code] = '{}000000000001'.format(village_code)
                    elif self.processed_address.get('town'):
                        town_code = self.processed_address.get('town_code')
                        self.processed_address[key_code] = '{}0000000000000001'.format(town_code)
                    elif self.processed_address.get('county'):
                        county_code = self.processed_address.get('county_code')
                        self.processed_address[key_code] = '{}00000000000000000001'.format(county_code)
                    else:
                        self.processed_address[key_code] = '{}000000000000000000000001'.format(self.processed_address.get('city_code'))
                elif level_name == 'floor':
                    if self.processed_address.get('unit'):
                        unit_code = self.processed_address.get('unit_code')
                        self.processed_address[key_code] = '{}0001'.format(unit_code)
                    elif self.processed_address.get('school'):
                        school_code = self.processed_address.get('school_code')
                        self.processed_address[key_code] = '{}00000001'.format(school_code)
                    elif self.processed_address.get('team'):
                        team_code = self.processed_address.get('team_code')
                        self.processed_address[key_code] = '{}000000000001'.format(team_code)
                    elif self.processed_address.get('village'):
                        village_code = self.processed_address.get('village_code')
                        self.processed_address[key_code] = '{}0000000000000001'.format(village_code)
                    elif self.processed_address.get('town'):
                        town_code = self.processed_address.get('town_code')
                        self.processed_address[key_code] = '{}00000000000000000001'.format(town_code)
                    elif self.processed_address.get('county'):
                        county_code = self.processed_address.get('county_code')
                        self.processed_address[key_code] = '{}000000000000000000000001'.format(county_code)
                    else:
                        self.processed_address[key_code] = '{}000000000000000000000001'.format(self.processed_address.get('city_code'))
                elif level_name == 'room':
                    if self.processed_address.get('floor'):
                        floor_code = self.processed_address.get('floor_code')
                        self.processed_address[key_code] = '{}0001'.format(floor_code)
                    elif self.processed_address.get('unit'):
                        unit_code = self.processed_address.get('unit_code')
                        self.processed_address[key_code] = '{}00000001'.format(unit_code)
                    elif self.processed_address.get('school'):
                        school_code = self.processed_address.get('school_code')
                        self.processed_address[key_code] = '{}000000000001'.format(school_code)
                    elif self.processed_address.get('team'):
                        team_code = self.processed_address.get('team_code')
                        self.processed_address[key_code] = '{}0000000000000001'.format(team_code)
                    elif self.processed_address.get('village'):
                        village_code = self.processed_address.get('village_code')
                        self.processed_address[key_code] = '{}00000000000000000001'.format(village_code)
                    elif self.processed_address.get('town'):
                        town_code = self.processed_address.get('town_code')
                        self.processed_address[key_code] = '{}000000000000000000000001'.format(town_code)
                    elif self.processed_address.get('county'):
                        county_code = self.processed_address.get('county_code')
                        self.processed_address[key_code] = '{}0000000000000000000000000001'.format(county_code)
                    else:
                        self.processed_address[key_code] = '{}0000000000000000000000000001'.format(self.processed_address.get('city_code'))
                if self.processed_address.get(level_name):
                    self.processed_address[level_name] = self.processed_address[level_name] + match
                else:
                    self.processed_address[level_name] = match
                return False
        self.processed_address[key_code] = ''
        self.processed_address[level_name] = ''
        return False

    def df_filter(self, value=None, criterion=None):
        if value in criterion:
            return criterion.dropna().drop_duplicates()

    def select_similar(self, s1, s2):
        import difflib
        s = difflib.get_close_matches(s1, s2, 1)
        # print('similare:', s)
        if len(s) == 1:
            return s[0]
        else:
            return None

    def adjust_changes(self, county=None):
        r = changes[changes['county_name_old'] == county]
        if not r.empty:
            county = r[:1]['county_name'].get_values()[0]
            self.match_county(county)
            return True
        else:
            return False

    def match_standard(self, group=None, match=None):
        """
        :param group: the top 5 group
        :param match: need to match value
        :return: False: 前4级在国标里没有匹配到, 等所有的地址都分组查找完后,如何remaining值不为空,才去百度API匹配
                 True: 表示当前级在国标里匹配到了,等所有的地址都分组查找完后,如何remaining值不为空,才去百度API匹配
        """

        level = len(self.processed_address.keys())
        # print("wait match:{}, group:{}, level:{}".format(match, group, level))
        if group in enumerate(self.keywords)
        if level == 0:
            return self.match_province(match)
        elif level == 2:
            return self.match_city(match)
        elif level == 4:
            return self.match_county(match)
        # elif level == 6 and group in self.keywords_4:
        elif level == 6:
            return self.match_town(match)
        # elif level == 8 and group in self.keywords_5:
        # elif level == 8:
        elif level == 8:
            # return self.match_custom_level('village', village, self.total_name[6], match)
            # return self.match_custom_level('village', village, match)
            return self.match_village(match)
        elif group in self.keywords_6:
            return self.match_custom_level('team', team, match)
            # return self.match_custom_level('group', team, self.total_name[8], match)
        elif group in self.keywords_7:
            return self.match_custom_level('school', school, match)
            # return self.match_custom_level('school', school, self.total_name[10], match)
        elif group in self.keywords_8:
            return self.match_custom_level('unit', unit, match)
            # return self.match_custom_level('unit', unit, self.total_name[12], match)
        elif group in self.keywords_9:
            return self.match_custom_level('floor', floor, match)
            # return self.match_custom_level('floor', floor, self.total_name[14], match)
        elif group in self.keywords_10:
            return self.match_custom_level('room', room, match)
            # return self.match_custom_level('room', room, self.total_name[16], match)
        else:
            print('level 5+')
            return False

    def match_province(self, province=None):
        """
        .get_values(): remove the index
        [0]: get the first value in the list
        .all(): the each column value of standard is not none
        :param province:
        :return:
        """
        r = standard[standard['province'] == province].drop_duplicates().dropna()
        if not r.empty:
            self.processed_address['province_code'] = r['province_code'].get_values()[0]
            self.processed_address['province'] = r['province'].get_values()[0]
            return True
        else:
            print('not matched')
            self.processed_address['province_code'] = ''
            self.processed_address['province'] = ''
            return False


    def match_city(self, city=None):
        if 'province' in self.processed_address.keys():
            r = standard[(standard['province'] == self.processed_address['province']) & (standard['city'] == city)]
            county = standard[(standard['province'] == self.processed_address['province'])
                              & (standard['county'] == city)]
        else:
            r = standard[standard['city'] == city]
            if 'city' in self.processed_address.keys():
                county = standard[(standard['city'] == self.processed_address['city'])
                                  & (standard['county'] == city)]
            else:
                county = standard[standard['county'] == city]

        if not r.empty:
            if 'provice' not in self.processed_address.keys():
                self.processed_address['province_code'] = '{:}'.format(r[:1]['province_code'].get_values()[0])
                self.processed_address['province'] = r[:1]['province'].get_values()[0]
            self.processed_address['city_code'] = '{}'.format(r[:1]['city_code'].get_values()[0])
            self.processed_address['city'] = r[:1]['city'].get_values()[0]
            return True
        elif not county.empty:
            city = county[:1]['city'].get_values()[0]
            city_code = county[:1]['city_code'].get_values()[0]
            c = county[:1]['county'].get_values()[0]
            county_code = county[:1]['county_code'].get_values()[0]
            if 'province' not in self.processed_address.keys():
                province_code = county[:1]['province_code'].get_values()[0]
                self.processed_address['province_code'] = '{}'.format(province_code)
                province = county[:1]['province'].get_values()[0]
                self.processed_address['province'] = province
            self.processed_address['city_code'] = '{}'.format(city_code)
            self.processed_address['city'] = city
            self.processed_address['county_code'] = county_code
            self.processed_address['county'] = c
            return True
        else:
            self.processed_address['city_code'] = ''
            self.processed_address['city'] = ''
            return False


    def match_county(self, county=None):
        if 'province' in self.processed_address.keys() and 'city' in self.processed_address.keys():
            r = standard[(standard['province'] == self.processed_address['province']) \
                     & (standard['city'] == self.processed_address['city']) & (standard['county'] == county)]
        if r.empty and 'province' in self.processed_address.keys():
            r = standard[(standard['county'] == county)
                         & (standard['province'] == self.processed_address['province'])]
        if not r.empty:
            if 'province' not in self.processed_address.keys():
                province = r[:1]['province'].get_values()[0]
                province_code = r[:1]['province_code'].get_values()[0]
                self.processed_address['province_code'] = province_code

                self.processed_address['province'] = province
            city = r['city'].get_values()[0]
            city_code = r[:1]['city_code'].get_values()[0]
            self.processed_address['city_code'] = city_code

            self.processed_address['city'] = city

            county_code = r[:1]['county_code'].get_values()[0]
            self.processed_address['county_code'] = county_code
            self.processed_address['county'] = county
            return True
        else:
            result = self.adjust_changes(county)
            if result:
                return True
            city_code = self.processed_address.get('city_code')
            if city_code:
                maxcode = int(
                    standard[standard.get('city_code') == city_code]['county_code'].dropna().max()) + 1
                self.processed_address['county_code'] = '{}'.format(maxcode)
            self.processed_address['county'] = county
            return False


    def match_town(self, town=None):
        if 'county' in self.processed_address.keys() and 'city' in self.processed_address.keys()\
                and 'province' in self.processed_address.keys():
            r = standard[(standard['province'] == self.processed_address['province']) \
                     & (standard['city'] == self.processed_address['city'])\
                     & (standard['county'] == self.processed_address['county'])
                     & (standard['town'] == town)]
        elif 'city' in self.processed_address.keys()\
                and 'province' in self.processed_address.keys():
            r = standard[(standard['province'] == self.processed_address['province']) \
                         & (standard['city'] == self.processed_address['city']) \
                         & (standard['town'] == town)]
        elif 'province' in self.processed_address.keys():
            r = standard[(standard['province'] == self.processed_address['province']) \
                         & (standard['town'] == town)]
        if not r.empty:
            if 'province' not in self.processed_address.keys():
                province = r[:1]['province'].get_values()[0]
                province_code = r[:1]['province_code'].get_values()[0]
                self.processed_address['province_code'] = province_code
                self.processed_address['province'] = province
            if 'city' not in self.processed_address.keys():
                city = r['city'].get_values()[0]
                city_code = r[:1]['city_code'].get_values()[0]
                self.processed_address['city_code'] = city_code
                self.processed_address['city'] = city
            if 'county' not in self.processed_address.keys():
                county = r['county'].get_values()[0]
                county_code = r[:1]['county_code'].get_values()[0]
                self.processed_address['county_code'] = county_code
                self.processed_address['county'] = county
            town_code = r[:1]['town_code'].get_values()[0]
            town = r[:1]['town'].get_values()[0]
            self.processed_address['town_code'] = town_code
            self.processed_address['town'] = town
            return True
        else:
            town = self.drop_duplicated_filed(town)
            r = standard[(standard['province'] == self.processed_address['province']) \
                         & (standard['city'] == self.processed_address['city']) \
                         & (standard['county'] == self.processed_address['county'])
                         & (standard['town'] == town)]
            if not r.empty:
                town_code = r[:1]['town_code'].get_values()[0]
                town = r[:1]['town'].get_values()[0]
                self.processed_address['town_code'] = town_code
                self.processed_address['town'] = town
                return True
            else:
                r = standard[(standard['province'] == self.processed_address['province']) \
                         & (standard['city'] == self.processed_address['city'])
                         & (standard['town'].str.contains(town))]
                if not r.empty:
                    county_code = r[:1]['county_code'].get_values()[0]
                    county = r[:1]['county'].get_values()[0]
                    self.processed_address['county_code'] = county_code
                    self.processed_address['county'] = county
                    town_code = r[:1]['town_code'].get_values()[0]
                    town = r[:1]['town'].get_values()[0]
                    self.processed_address['town_code'] = town_code
                    self.processed_address['town'] = town
                else:
                    county_code = self.processed_address.get('county_code')
                    # print(standard.get('county_code'), county_code)
                    if county_code:
                        maxcode = standard[standard.get('county_code') == county_code]['town_code'].dropna().astype(int).max() + 1
                        if not pd.isna(maxcode):
                            self.processed_address['town_code'] = '{}'.format(maxcode)
                        else:
                            self.processed_address['town_code'] = '{}001'.format(county_code)
                    else:
                        city_code = self.processed_address.get('city_code')
                        if city_code:
                            maxcode = int(
                                standard[standard.get('city_code') == city_code]['town_code'].dropna().max()) + 1
                            if not pd.isna(maxcode):
                                self.processed_address['town_code'] = '{}'.format(maxcode)
                            else:
                                self.processed_address['town_code'] = '{}00001'.format(city_code)
                    self.processed_address['town'] = town
                return False
            
    def match_village(self, village=None):
        r = area_village.get('name').str.contains(village)
        if r:
            self.processed_address.update(self.processed_address.get(self.total_name[])+village)

            
    def area_url_search(self, values=None, region=None):
        print('area search:"{}, region:{}'.format(values, region))
        areurl = self.areurl.format(values, region)
        r = requests.get(areurl + self.token)
        response_dict = r.json()
        print("response:", response_dict)
        if response_dict and 'results' in response_dict.keys() and len(response_dict['results']) != 0:
            for a in response_dict['results']:
                if a.get('name'):
                    name = a['name']
                    name = self.drop_invalid_characters(name)
                    name = self.drop_duplicated_filed(name)
                else:
                    name = ''
                lat = a['location']['lat']
                lng = a['location']['lng']
                if a.get('address'):
                    address = a['address']
                    address = self.drop_invalid_characters(address)
                    address = self.drop_duplicated_filed(address)
                else:
                    address = ''
                if a.get('province'):
                    province = a['province']
                else:
                    province = self.processed_address.get('province')
                if a.get('city'):
                    city = a['city']
                else:
                    city = self.processed_address.get('city')
                if a.get('area'):
                    area = a['area']
                else:
                    area = self.processed_address.get('county')
                if 'detail_info' in a.keys() and 'tag' in a['detail_info'].keys():
                    address_type = a['detail_info']['tag']
                else:
                    address_type = ''
                    
                match = address + name
                pre_end = {}

                pattern = re.finditer(self.keywords_4 + '|' + self.keywords_other, match)
                # remaining = match
                row = np.random.randint(1, 10000)
                for i, g in enumerate(pattern):
                    pre_end[i] = g.end()
                    extract = match[:g.end()]
                    if i != 0:
                        extract = match[pre_end[i-1]:g.end()]
                    if extract == g.group():
                        continue
                    if len(extract) != 0:
                        extract = self.drop_invalid_characters(extract)
                    if len(extract) != 0:
                        middle = self.drop_duplicated_filed(extract)
                    if len(middle) != 0:
                        if area == self.processed_address.get('county') and province == self.processed_address.get('province')\
                                and city and self.processed_address.get('city'):
                            if g.group() in self.keywords_4 and not self.processed_address.get('town'):
                                county_code = self.processed_address.get('county_code')
                                if county_code:
                                    maxcode = standard[standard.get('county_code') == county_code][
                                                  'town_code'].dropna().astype(int).max() + 1
                                    self.processed_address['town_code'] = '{}'.format(maxcode)
                                else:
                                    city_code = self.processed_address.get('city_code')
                                    if city_code:
                                        maxcode = int(
                                            standard[standard.get('city_code') == city_code][
                                                'town_code'].dropna().max()) + 1
                                        self.processed_address['town_code'] = '{}'.format(maxcode)
                                self.processed_address['town'] = middle
                            elif g.group() in self.keywords_5 and not self.processed_address.get('village'):
                                town_code = self.processed_address.get('town_code')
                                county_code = self.processed_address.get('county_code')
                                if town_code:
                                    # print('-------------', standard[standard.get('town_code') == town_code]['town_code'].dropna().astype(int).max())
                                    maxcode = standard[standard.get('town_code') == town_code]['town_code'].dropna().astype(int).max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['village_code'] = '{}0001'.format(maxcode)
                                elif county_code:
                                    maxcode = standard[standard.get('town_code') == town_code]['town_code'].dropna().astype(int).max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['village_code'] = '{}0001'.format(maxcode)
                                else:
                                    city_code = self.processed_address.get('city_code')
                                    if city_code:
                                        maxcode = standard[standard.get('town_code') == town_code][
                                                      'town_code'].dropna().astype(int).max() + 1
                                        if not pd.isna(maxcode):
                                            self.processed_address['village_code'] = '{}0001'.format(maxcode)
                                self.processed_address['village'] = middle
                            elif g.group() in self.keywords_6 and not self.processed_address.get('team'):
                                village_code = self.processed_address.get('village_code')
                                town_code = self.processed_address.get('town_code')
                                county_code = self.processed_address.get('county_code')
                                if village_code:  # 有待完善
                                    # maxcode = standard[self.team.get('parent_code') == village_code][
                                    #               'code'].dropna().astype(int).max() + 1
                                    self.processed_address['team_code'] = '{}0001'.format(village_code)
                                elif town_code:
                                    maxcode = standard[standard.get('town_code') == town_code]['town_code'].dropna().astype(int).max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['team_code'] = '{}00000001'.format(maxcode)
                                elif county_code:
                                    maxcode = standard[standard.get('town_code') == town_code]['town_code'].dropna().astype(int).max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['team_code'] = '{}00000001'.format(maxcode)
                                else:
                                    city_code = self.processed_address.get('city_code')
                                    if city_code:
                                        maxcode = standard[standard.get('town_code') == town_code][
                                                      'town_code'].dropna().astype(int).max() + 1
                                        if not pd.isna(maxcode):
                                            self.processed_address['team_code'] = '{}00000001'.format(maxcode)
                                self.processed_address['team'] = middle
                            elif g.group() in self.keywords_7 and not self.processed_address.get('school'):
                                team_code = self.processed_address.get('team_code')
                                village_code = self.processed_address.get('village_code')
                                town_code = self.processed_address.get('town_code')
                                county_code = self.processed_address.get('county_code')
                                if team_code:
                                    self.processed_address['school_code'] = '{}0001'.format(team_code)
                                elif village_code:  # 有待完善
                                    # maxcode = standard[self.team.get('parent_code') == village_code][
                                    #               'code'].dropna().astype(int).max() + 1
                                    self.processed_address['school_code'] = '{}00000001'.format(village_code)
                                elif town_code:
                                    maxcode = standard[standard.get('town_code') == town_code]['town_code'].dropna().astype(int).max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['school_code'] = '{}000000000001'.format(maxcode)
                                elif county_code:
                                    maxcode = standard[standard.get('town_code') == town_code]['town_code'].dropna().astype(int).max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['school_code'] = '{}000000000001'.format(maxcode)
                                else:
                                    city_code = self.processed_address.get('city_code')
                                    if city_code:
                                        maxcode = standard[standard.get('town_code') == town_code][
                                                      'town_code'].dropna().astype(int).max() + 1
                                        if not pd.isna(maxcode):
                                            self.processed_address['school_code'] = '{}000000000001'.format(maxcode)
                                self.processed_address['school'] = middle
                            elif g.group() in self.keywords_8 and not self.processed_address.get('unit'):
                                school_code = self.processed_address.get('school_code')
                                team_code = self.processed_address.get('team_code')
                                village_code = self.processed_address.get('village_code')
                                town_code = self.processed_address.get('town_code')
                                county_code = self.processed_address.get('county_code')
                                if school_code:
                                    self.processed_address['school_code'] = '{}0001'.format(school_code)
                                elif team_code:
                                    self.processed_address['team_code'] = '{}0001'.format(team_code)
                                elif village_code:  # 有待完善
                                    # maxcode = standard[self.team.get('parent_code') == village_code][
                                    #               'code'].dropna().astype(int).max() + 1
                                    self.processed_address['team_code'] = '{}0001'.format(village_code)
                                elif town_code:
                                    maxcode = standard[standard.get('town_code') == town_code]['town_code'].dropna().astype(int).max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['unit_code'] = '{}0000000000000001'.format(maxcode)
                                elif county_code:
                                    maxcode = standard[standard.get('town_code') == town_code]['town_code'].dropna().astype(int).max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['unit_code'] = '{}0000000000000001'.format(maxcode)
                                else:
                                    city_code = self.processed_address.get('city_code')
                                    if city_code:
                                        maxcode = standard[standard.get('town_code') == town_code][
                                                      'town_code'].dropna().astype(int).max() + 1
                                        if not pd.isna(maxcode):
                                            self.processed_address['unit_code'] = '{}0000000000000001'.format(maxcode)
                                self.processed_address['unit'] = middle
                            elif g.group() in self.keywords_9 and not self.processed_address.get('floor'):
                                unit_code = self.processed_address.get('unit_code')
                                school_code = self.processed_address.get('school_code')
                                team_code = self.processed_address.get('team_code')
                                village_code = self.processed_address.get('village_code')
                                town_code = self.processed_address.get('town_code')
                                county_code = self.processed_address.get('county_code')
                                if unit_code:
                                    self.processed_address['unit_code'] = '{}0001'.format(unit_code)
                                elif school_code:
                                    self.processed_address['school_code'] = '{}0001'.format(school_code)
                                elif team_code:
                                    self.processed_address['team_code'] = '{}0001'.format(team_code)
                                elif village_code:  # 有待完善
                                    # maxcode = standard[self.team.get('parent_code') == village_code][
                                    #               'code'].dropna().astype(int).max() + 1
                                    self.processed_address['team_code'] = '{}0001'.format(village_code)
                                elif town_code:
                                    maxcode = standard[standard.get('town_code') == town_code][
                                                  'town_code'].dropna().max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['floor_code'] = '{}00000000000000000001'.format(maxcode)
                                elif county_code:
                                    maxcode = standard[standard.get('county_code') == county_code][
                                                  'town_code'].dropna().max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['floor_code'] = '{}00000000000000000001'.format(maxcode)
                                else:
                                    city_code = self.processed_address.get('city_code')
                                    if city_code:
                                        maxcode = int(
                                            standard[standard.get('city_code') == city_code][
                                                'town_code'].dropna().max()) + 1
                                        if not pd.isna(maxcode):
                                            self.processed_address['floor_code'] = '{}00000000000000000001'.format(maxcode)
                                self.processed_address['floor'] = middle
                            elif g.group() in self.keywords_10 and not self.processed_address.get('room'):
                                floor_code = self.processed_address.get('floor_code')
                                unit_code = self.processed_address.get('unit_code')
                                school_code = self.processed_address.get('school_code')
                                team_code = self.processed_address.get('team_code')
                                village_code = self.processed_address.get('village_code')
                                town_code = self.processed_address.get('town_code')
                                county_code = self.processed_address.get('county_code')
                                if floor_code:
                                    self.processed_address['room_code'] = '{}0001'.format(floor_code)
                                elif unit_code:
                                    self.processed_address['unit_code'] = '{}0001'.format(unit_code)
                                elif school_code:
                                    self.processed_address['school_code'] = '{}0001'.format(school_code)
                                elif team_code:
                                    self.processed_address['team_code'] = '{}0001'.format(team_code)
                                elif village_code:  # 有待完善
                                    # maxcode = standard[self.team.get('parent_code') == village_code][
                                    #               'code'].dropna().astype(int).max() + 1
                                    self.processed_address['village_code'] = '{}0001'.format(village_code)
                                elif town_code:
                                    maxcode = standard[standard.get('town_code') == town_code][
                                                  'town_code'].dropna().max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['room_code'] = '{}000000000000000000000001'.format(maxcode)
                                elif county_code:
                                    maxcode = standard[standard.get('county_code') == county_code][
                                                  'town_code'].dropna().max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['room_code'] = '{}000000000000000000000001'.format(maxcode)
                                else:
                                    city_code = self.processed_address.get('city_code')
                                    if city_code:
                                        maxcode = int(
                                            standard[standard.get('city_code') == city_code][
                                                'town_code'].dropna().max()) + 1
                                        if not pd.isna(maxcode):
                                            self.processed_address['room_code'] = '{}000000000000000000000001'.format(
                                            maxcode)

                                self.processed_address['room'] = middle
                        else:  #不在一个区的

                            # if g.group() in self.keywords_3:
                            #     self.baidu_df.loc[i, 'county_code'] = self.processed_address.get('county_code')
                            #     self.baidu_df.loc[i, 'town'] = middle
                            if g.group() in self.keywords_5:
                                self.baidu_df.loc[row, 'village_code'] = self.processed_address.get('village_code')
                                self.baidu_df.loc[row, 'village'] = middle
                            elif g.group() in self.keywords_6:
                                self.baidu_df.loc[row, 'team_code'] = self.processed_address.get('team_code')
                                self.baidu_df.loc[row, 'team'] = middle
                            elif g.group() in self.keywords_7:
                                self.baidu_df.loc[row, 'school_code'] = self.processed_address.get('school_code')
                                self.baidu_df.loc[row, 'school'] = middle
                            elif g.group() in self.keywords_8:
                                self.baidu_df.loc[row, 'unit_code'] = self.processed_address.get('unit_code')
                                self.baidu_df.loc[row, 'unit'] = middle
                            elif g.group() in self.keywords_9:
                                self.baidu_df.loc[row, 'floor_code'] = self.processed_address.get('floor_code')
                                self.baidu_df.loc[row, 'floor'] = middle
                            elif g.group() in self.keywords_10:
                                self.baidu_df.loc[row, 'room_code'] = self.processed_address.get('room_code')
                                self.baidu_df.loc[row, 'room'] = middle
                cols = self.baidu_df.shape[0]
                print('cols:', cols)
                if cols >= 2:
                    self.baidu_df.loc[row, 'type'] = address_type
                # self.baidu_df.loc[np.random.randint(1, 10000), self.processed_address.keys()] = self.processed_address.values()
                # self.baidu_df.from_dict(self.processed_address)
                # print('baidu:', self.baidu_df)

                if 'province' in self.processed_address.keys() and 'city' in self.processed_address.keys() \
                        and 'county' in self.processed_address.keys() and 'town' in self.processed_address.keys():
                    self.processed_address['type'] = address_type
                    return False, lat, lng
                else:
                    return True, lat, lng
        else:
            pattern = re.finditer(self.keywords_other, values)
            for i, g in enumerate(pattern):
                print("group:{}".format(g.group()))
                extract = values[:g.end()]
                remaining = values[g.end():]
                r = self.drop_invalid_characters(extract)
                if len(r) != 0:
                    r = extract.replace(g.group(), '')
                if len(r) != 0:
                    r = re.sub(self.digialpha, '', r)
                print('search: ex:{}, rem:{}'.format(extract, remaining))

                if len(r) != 0:
                    r, lat, lng = self.area_url_search(r, region)
                    print('seconde area:', r, lat, lng)
                    return r, lat, lng
                else:
                    continue
            return False, None, None


    def circle_url_search(self, value=None, lat=None, lng=None, r=500):
        circle_url = self.circle_url.format("街道$乡$镇$社区$村", lat, lng)
        print("search circle_url 1:{}".format(circle_url))
        r = requests.get(circle_url + self.token)
        response_dict = r.json()
        print("response circle_url :", response_dict['results'])
        address_type = None
        if response_dict and len(response_dict['results']) != 0:
            for a in response_dict['results']:
                name = a['name']
                address = a['address']
                if '街道' in name:
                    town = name[:name.find('街道')] + '街道'
                    return town, address_type
                if '乡' in name:
                    town = name[:name.find('乡')] + '乡'
                    return town, address_type
                if '镇' in name:
                    town = name[:name.find('镇')] + '镇'
                    return town, address_type
            else:
                print('no 街道乡镇')

    def search_baidu(self, value=None, region=None):
        r, lat, lng = self.area_url_search(value, region)
        print('result area:', r, lat, lng)
        if r:
           self.circle_url_search(value, lat, lng, r)
        # else:
        #     self.processed_address['room'] = value
            # import uuid
            # namespace = uuid.NAMESPACE_DNS
            # code = uuid.uuid3(namespace, region + value)
            # self.processed_address['level_other_code'] = code


def read_standard_data():
    global standard
    standard = pd.read_csv('area_code.csv', low_memory=False, dtype=str)
    global area_village
    area_village = pd.read_csv('area_village.csv', low_memory=False, dtype=str, names=['parent_code', 'name', 'code'])
    global changes
    c = pd.read_csv('name_change.csv', low_memory=False)
    changes = c.filter(items=['county_name_old', 'county_name'])

    output_path = '../output_data/'
    try:
        global village
        village = pd.read_csv('{}{}'.format(output_path, 'village.csv'), names=['parent_code', 'name', 'code'], sep='\t', low_memory=False,
                              dtype=str).drop_duplicates()
    except Exception as e:
        print('read e:{}'.format(e))
        village = None
    try:
        global school
        school = pd.read_csv('{}{}'.format(output_path, 'school.csv'), names=['parent_code', 'name', 'code'], sep='\t',  low_memory=False,
                             dtype=str).drop_duplicates()
    except Exception as e:
        print('read e:{}'.format(e))
        school = None
    try:
        global team
        team = pd.read_csv('{}{}'.format(output_path, 'team.csv'), names=['parent_code', 'name', 'code'], sep='\t',
                                   low_memory=False, dtype=str).drop_duplicates()
    except Exception as e:
        team = None
        print('read e:{}'.format(e))
    try:
        global unit
        unit = pd.read_csv('{}{}'.format(output_path, 'unit.csv'), names=['parent_code', 'name', 'code'], sep='\t',
                               low_memory=False, dtype=str).drop_duplicates()
    except Exception as e:
        unit = None
        print('read e:{}'.format(e))
    try:
        global floor
        floor = pd.read_csv('{}{}'.format(output_path, 'floor.csv'), names=['parent_code', 'name', 'code'], sep='\t',
                            low_memory=False, dtype=str).drop_duplicates()
    except Exception as e:
        floor = None
        print('read e:{}'.format(e))
    try:
        global room
        room = pd.read_csv('{}{}'.format(output_path, 'room.csv'), names=['parent_code', 'name', 'code'], sep='\t',
                           low_memory=False, dtype=str).drop_duplicates()

    except Exception as e:
        print('read e:{}'.format(e))
        room = None


def read_file_multipool():
    pdf = pd.read_csv('../input_data/test_file_2.csv', sep='\t', header=None, iterator=True,
                      names=['id', 'recipient_prov_name', 'recipient_city_name',
                             'recipient_county_name', 'recipient_name', 'recipient_mobile', 'order_create_time',
                             'recipient_address'])
    loop = True
    chunk_size = 1000
    read_standard_data()
    ap = AddressProcess()
    while loop:
        try:
            chunk = pdf.get_chunk(chunk_size)
            if chunk is not None:
                ap.raw_process(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")


if __name__ == '__main__':
    read_file_multipool()