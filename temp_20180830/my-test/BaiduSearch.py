# encoding: utf-8
"""
@version: python 3.6
@author:  YTO
@license: Apache Licence 
@time: 2018/8/17
"""

import requests
import re
import pandas as pd
import numpy as np

class BaiduSearch(object):
    def __init__(self):
        # 行政区划区域检索
        self_baidu_tag_1 = ['美食','酒店','购物','生活服务','丽人','旅游景点','休闲娱乐','运动健身','教育培训','文化传媒','医疗','汽车服务','交通设施','金融','房地产','公司企业','政府机构','出入口','自然地物']

        self.area_url = 'http://api.map.baidu.com/place/v2/search?query={}&region={}&city_limit=true&scope=2' \
                        '&page_size=20&page_num=0&output=json&ak='
        self.circle_url = "http://api.map.baidu.com/place/v2/search?query={}&location={},{}&radius=500&output=json&ak="
        self.token = "UauOmu44P0TQEkOC8RMInMFwAFDm7dXd"
        self.total_name = ['province_code', 'province', 'city_code', 'city', 'county_code', 'county', 'town_code',
                           'town', 'village_code', 'village', 'team_code', 'team', 'school_code', 'school',
                           'unit_code', 'unit', 'floor_code', 'floor', 'room_code', 'room', 'type']
        self.baidu_df = pd.DataFrame(columns=self.total_name)
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
        self.keywords = self.keywords_1 + '|' + self.keywords_2 + '|' + self.keywords_3 + '|' \
                        + self.keywords_4 + '|' + self.keywords_5 + '|' + self.keywords_6 + '|' + self.keywords_7 + '|' \
                        + self.keywords_8 + '|' + self.keywords_9 + '|' + self.keywords_10
        # self.total_keywords = [self.keywords_1, self.keywords_2, self.keywords_3, self.keywords_4, self.keywords_5,
        #                        self.keywords_6, self.keywords_7, self.keywords_8, self.keywords_9, self.keywords_10]
        self.keywords_other = self.keywords_5 + '|' + self.keywords_6 + '|' + self.keywords_7 + '|' \
                              + self.keywords_8 + '|' + self.keywords_9 + '|' + self.keywords_10

        self.processed_address = {}

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

    def area_url_search(self, base=0, values=None, region=None):
        print('area search:{}, region:{}'.format(values, region))
        area_url = self.area_url.format(values, region)
        print('area_url:{}'.format(area_url))
        r = requests.get(area_url + self.token)
        response_dict = r.json()
        print("response:", response_dict)

        if response_dict and 'results' in response_dict.keys() and len(response_dict['results']) != 0:
            for i, a in enumerate(response_dict['results']):
                row = base + i + np.random.randint(9999)
                if a.get('name'):
                    name = a['name']
                    lat = a['location']['lat']
                    lng = a['location']['lng']
                    # name = self.drop_invalid_characters(name)
                    # name = self.drop_duplicated_filed(name)
                else:
                    name = ''
                return lat, lng
                # lat = a['location']['lat']
                # lng = a['location']['lng']
                if a.get('address'):
                    address = a['address']
                    # address = self.drop_invalid_characters(address)
                    # address = self.drop_duplicated_filed(address)
                else:
                    address = ''
                # self.circle_url_search(lat=lat, lng=lng)
                # if a.get('province'):
                #     province = a['province']
                # else:
                #     province = self.processed_address.get('province')
                # if a.get('city'):
                #     city = a['city']
                # else:
                #     city = self.processed_address.get('city')
                # if a.get('area'):
                #     area = a['area']
                # else:
                #     area = self.processed_address.get('county')
                if 'detail_info' in a.keys() and 'tag' in a['detail_info'].keys():
                    address_type = a['detail_info']['tag']
                else:
                    address_type = ''

                self.baidu_df.loc[row, 'village'] = address
                self.baidu_df.loc[row, 'team'] = name
                self.baidu_df.loc[row, 'type'] = address_type
                continue
                match = address + name
                pre_end = {}

                pattern = re.finditer(self.keywords_4 + '|' + self.keywords_other, match)
                # remaining = match
                row = np.random.randint(1, 10000)
                for i, g in enumerate(pattern):
                    pre_end[i] = g.end()
                    extract = match[:g.end()]
                    if i != 0:
                        extract = match[pre_end[i - 1]:g.end()]
                    if extract == g.group():
                        continue
                    if len(extract) != 0:
                        extract = self.drop_invalid_characters(extract)
                    if len(extract) != 0:
                        middle = self.drop_duplicated_filed(extract)
                    if len(middle) != 0:
                        if area == self.processed_address.get('county') and province == self.processed_address.get(
                                'province') \
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
                                    maxcode = standard[standard.get('town_code') == town_code][
                                                  'town_code'].dropna().astype(int).max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['village_code'] = '{}0001'.format(maxcode)
                                elif county_code:
                                    maxcode = standard[standard.get('town_code') == town_code][
                                                  'town_code'].dropna().astype(int).max() + 1
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
                                    maxcode = standard[standard.get('town_code') == town_code][
                                                  'town_code'].dropna().astype(int).max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['team_code'] = '{}00000001'.format(maxcode)
                                elif county_code:
                                    maxcode = standard[standard.get('town_code') == town_code][
                                                  'town_code'].dropna().astype(int).max() + 1
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
                                    maxcode = standard[standard.get('town_code') == town_code][
                                                  'town_code'].dropna().astype(int).max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['school_code'] = '{}000000000001'.format(maxcode)
                                elif county_code:
                                    maxcode = standard[standard.get('town_code') == town_code][
                                                  'town_code'].dropna().astype(int).max() + 1
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
                                    maxcode = standard[standard.get('town_code') == town_code][
                                                  'town_code'].dropna().astype(int).max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['unit_code'] = '{}0000000000000001'.format(maxcode)
                                elif county_code:
                                    maxcode = standard[standard.get('town_code') == town_code][
                                                  'town_code'].dropna().astype(int).max() + 1
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
                                            self.processed_address['floor_code'] = '{}00000000000000000001'.format(
                                                maxcode)
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
                                        self.processed_address['room_code'] = '{}000000000000000000000001'.format(
                                            maxcode)
                                elif county_code:
                                    maxcode = standard[standard.get('county_code') == county_code][
                                                  'town_code'].dropna().max() + 1
                                    if not pd.isna(maxcode):
                                        self.processed_address['room_code'] = '{}000000000000000000000001'.format(
                                            maxcode)
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
                        else:  # 不在一个区的

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
            # pattern = re.finditer(self.keywords_other, values)
            # for i, g in enumerate(pattern):
            #     print("group:{}".format(g.group()))
            #     extract = values[:g.end()]
            #     remaining = values[g.end():]
            #     # r = self.drop_invalid_characters(extract)
            #     if len(r) != 0:
            #         r = extract.replace(g.group(), '')
            #     if len(r) != 0:
            #         r = re.sub(self.digialpha, '', r)
            #     print('search: ex:{}, rem:{}'.format(extract, remaining))
            #
            #     if len(r) != 0:
            #         r, lat, lng = self.area_url_search(r, region)
            #         print('seconde area:', r, lat, lng)
            #         return r, lat, lng
            #     else:
            #        continue
            return False, None, None

    def circle_url_search(self, base=0, value=None, lat=None, lng=None, r=2000):
        # circle_url = self.circle_url.format("街道$乡$镇$社区$村", lat, lng)
        # circle_url = self.circle_url.format("教育培训$公司企业$政府机构$出入口$房地产$社区", lat, lng)
        circle_url = self.circle_url.format(value, lat, lng)

        print("search circle_url 1:{}".format(circle_url))
        r = requests.get(circle_url + self.token)
        response_dict = r.json()
        print("response circle_url :", response_dict['results'])
        address_type = None
        if response_dict and len(response_dict['results']) != 0:
            for i, a in enumerate(response_dict['results']):
                row = base + i + np.random.randint(9999)
                name = a['name']
                address = a['address']
                self.baidu_df.loc[row, 'team'] = address
                self.baidu_df.loc[row, 'shool'] = name
            else:
                print('no 街道乡镇')


def read_standard_data():
    global standard
    standard = pd.read_csv('../source/area_code.csv', low_memory=False, dtype=str)
    global area_village
    area_village = pd.read_csv('../source/area_village.csv', header=1, low_memory=False, dtype=str, names=['parent_code', 'name', 'code'])
    # global changes
    # c = pd.read_csv('name_change.csv', low_memory=False)
    # changes = c.filter(items=['county_name_old', 'county_name'])
    # global baidu_city
    # baidu_city = pd.read_csv('../source/BaiduMap_cityCode_1102.txt', header=0, names=['id', 'name'])

    output_path = '../baidu_output/'
    # try:
    #     global village
    #     village = pd.read_csv('{}{}'.format(output_path, 'village.csv'), names=['parent_code', 'name', 'code'], sep='\t', low_memory=False,
    #                           dtype=str).drop_duplicates()
    # except Exception as e:
    #     print('read e:{}'.format(e))
    #     village = None
    # try:
    #     global school
    #     school = pd.read_csv('{}{}'.format(output_path, 'school.csv'), names=['parent_code', 'name', 'code'], sep='\t',  low_memory=False,
    #                          dtype=str).drop_duplicates()
    # except Exception as e:
    #     print('read e:{}'.format(e))
    #     school = None
    # try:
    #     global team
    #     team = pd.read_csv('{}{}'.format(output_path, 'team.csv'), names=['parent_code', 'name', 'code'], sep='\t',
    #                                low_memory=False, dtype=str).drop_duplicates()
    # except Exception as e:
    #     team = None
    #     print('read e:{}'.format(e))
    # try:
    #     global unit
    #     unit = pd.read_csv('{}{}'.format(output_path, 'unit.csv'), names=['parent_code', 'name', 'code'], sep='\t',
    #                            low_memory=False, dtype=str).drop_duplicates()
    # except Exception as e:
    #     unit = None
    #     print('read e:{}'.format(e))
    # try:
    #     global floor
    #     floor = pd.read_csv('{}{}'.format(output_path, 'floor.csv'), names=['parent_code', 'name', 'code'], sep='\t',
    #                         low_memory=False, dtype=str).drop_duplicates()
    # except Exception as e:
    #     floor = None
    #     print('read e:{}'.format(e))
    # try:
    #     global room
    #     room = pd.read_csv('{}{}'.format(output_path, 'room.csv'), names=['parent_code', 'name', 'code'], sep='\t',
    #                        low_memory=False, dtype=str).drop_duplicates()
    #
    # except Exception as e:
    #     print('read e:{}'.format(e))
    #     room = None

if __name__ == '__main__':
    bs = BaiduSearch()
    read_standard_data()
    # city = standard.get(['city', 'city_code']).get_values()
    # city_code = standard.city_code.get_values()
    # village = area_village.name
    # for cc in city:
    #     name = cc[0]
    #     code = cc[1]
    #     # print(name, code)
    #     village = area_village[area_village.parent_code.str.startswith(code)].name
    #     for val in village:
    #         # print('----val----\n', val)
    #         bs.search_baidu(val, name)
    #     bs.baidu_df.to_csv('../baidu_output/baidu_data.csv', index=False, sep='\t')
    # print(city.get_values())
    # print('----\n', town_code)
    # print('-----village----\n', village.get_values())
    #
    # search = standard.get(['city', 'town', 'town_code']).get_values()
    # for s in search:
    #     region = s[0]
    #     region = '西安市雁塔区'
    #     query = s[1]
    #     code = s[2]
    #     village = area_village[area_village.parent_code.str.startswith(code)].name
    #     for v in village:
    #         bs.area_url_search(v, values=query, region=region)
    #     bs.baidu_df.to_csv('../baidu_output/baidu_data.csv', index=False, sep='\t')
    value = ['小寨路街道', '大雁塔街道', '长延堡街道', '电子城街道', '等驾坡街道', '鱼化寨街道', '丈八沟街道', '曲江街道']
    region = '西安市雁塔区'
    town = ['高新路', '锦业路', '丈八西', '锦业二路', '瞪羚一路', '科技八路', '铺尚新村', '高新六路', '丈八北路', ]
    tag = '路口$社区$村'
    for i, v in enumerate(value):
        lat, lng = bs.area_url_search(base=i, values=v, region=region)
        bs.circle_url_search(i, tag, lat, lng)
        bs.baidu_df.to_csv('../baidu_output/baidu_data_street_child.csv', index=False, sep='\t')
