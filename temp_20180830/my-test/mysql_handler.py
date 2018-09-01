# python3
# -*- coding: utf-8 -*-
import pymysql.cursors


class MysqlHandler(object):
    def __init__(self, database='test'):
        self.db = pymysql.connect(host="localhost", user="root", passwd="yto2018", db=database, charset="utf8", cursorclass=pymysql.cursors.DictCursor)

    def insert(self, level, name=None, parent_code=None, code=None):
        try:
            with self.db.cursor() as cursor:
                if level == 1:
                    cursor.execute('REPLACE INTO province (province_name, province_code) VALUES (%s, %s)', [name, code])
                elif level == 2:
                    cursor.execute('REPLACE INTO city (city_name, province_code, city_code) VALUES (%s, %s, %s)'
                                   , [name, parent_code, code])
                elif level == 3:
                    cursor.execute('REPLACE INTO county (county_name, city_code, county_code) VALUES (%s, %s, %s)'
                                   '', [name, parent_code, code])
                elif level == 4:
                    cursor.execute('REPLACE INTO town (town_name, county_code, town_code) VALUES (%s, %s, %s)'
                                   , [name, parent_code, code])
                elif level == 5:
                    cursor.execute('REPLACE INTO village (village_name, town_code, village_code) VALUES (%s, %s, %s)'
                                   , [name, parent_code, code])
                elif level == 6:
                    cursor.execute('REPLACE INTO street (street_name, village_code, street_code) VALUES (%s, %s, %s)'
                                   , [name, parent_code, code])
                elif level == 7:
                    cursor.execute('REPLACE INTO number (number_name, street_code, number_code) VALUES (%s, %s, %s)'
                                   , [name, parent_code, code])
                elif level == 8:
                    cursor.execute('REPLACE INTO building (building_name, number_code, building_code) VALUES (%s, %s, %s)'
                                   , [name, parent_code, code])
                elif level == 9:
                    cursor.execute('REPLACE INTO unit (unit_name, building_code, unit_code) VALUES (%s, %s, %s)'
                                   , [name, parent_code, code])
                elif level == 10:
                    cursor.execute('REPLACE INTO room (room_name, unit_code, room_code) VALUES (%s, %s, %s)'
                                   , [name, parent_code, code])
                elif level == 66: # insert total processed address
                    cursor.execute(name)
                else:  # 99, insert error url
                    cursor.execute(name)
            insert_id = cursor.lastrowid
            self.db.commit()
        except Exception as e:
            raise Exception('MySQL ERROR:', e)
        return insert_id

    def close(self):
        self.db.close()

    def truncate(self, table=None):
        try:
            with self.db.cursor() as cursor:
                cursor.execute('TRUNCATE `province`')
                cursor.execute('TRUNCATE `city`')
                cursor.execute('TRUNCATE `county`')
                cursor.execute('TRUNCATE `town`')
                cursor.execute('TRUNCATE `village`')
                # cursor.execute('TRUNCATE `remaining`')
                cursor.execute('TRUNCATE `street`')
                cursor.execute('TRUNCATE `number`')
                cursor.execute('TRUNCATE `building`')
                cursor.execute('TRUNCATE `unit`')
                cursor.execute('TRUNCATE `room`')
                cursor.execute('TRUNCATE `processed_total`')
            self.db.commit()
        except Exception as e:
            raise Exception('Mysql truncate error:', e)

    def fetch(self, sql):
        with self.db.cursor() as cursor:
            cursor.execute(sql)
            one = cursor.fetchall()
            return one

