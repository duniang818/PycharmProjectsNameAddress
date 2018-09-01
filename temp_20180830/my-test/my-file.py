# encoding: utf-8
"""
@version: python 3.6
@author:  wym
@license: Apache Licence 
@time: 2018/8/30
"""
import pandas as pd
output_path = '../output_data/'
new_path = '{}new/'.format(output_path)
def drop_file_dupicate():
    try:
        global village
        village = pd.read_csv('{}{}'.format(output_path, 'village.csv'), names=['parent_code', 'name', 'code'], sep='\t', low_memory=False,
                              dtype=str).drop_duplicates()
        village.to_csv('{}{}'.format(new_path, 'village.csv'), index=False, header=None)
    except Exception as e:
        print('read e:{}'.format(e))
        village = None
    try:
        global school
        school = pd.read_csv('{}{}'.format(output_path, 'school.csv'), names=['parent_code', 'name', 'code'], sep='\t',  low_memory=False,
                             dtype=str).drop_duplicates()
        school.to_csv('{}{}'.format(new_path, 'school.csv'), index=False)
    except Exception as e:
        print('read e:{}'.format(e))
        school = None
    try:
        global team
        team = pd.read_csv('{}{}'.format(output_path, 'team.csv'), names=['parent_code', 'name', 'code'], sep='\t',
                                   low_memory=False, dtype=str).drop_duplicates()
        team.to_csv('{}{}'.format(new_path, 'team.csv'), index=False)
    except Exception as e:
        team = None
    try:
        global unit
        unit = pd.read_csv('{}{}'.format(output_path, 'unit.csv'), names=['parent_code', 'name', 'code'], sep='\t',
                               low_memory=False, dtype=str).drop_duplicates()
        unit.to_csv('{}{}'.format(new_path, 'unit.csv'), index=False)
    except Exception as e:
        unit = None
    try:
        global floor
        floor = pd.read_csv('{}{}'.format(output_path, 'floor.csv'), names=['parent_code', 'name', 'code'], sep='\t',
                            low_memory=False, dtype=str).drop_duplicates()
        floor.to_csv('{}{}'.format(new_path, 'floor.csv'), index=False)
    except Exception as e:
        floor = None
    try:
        global room
        room = pd.read_csv('{}{}'.format(output_path, 'room.csv'), names=['parent_code', 'name', 'code'], sep='\t',
                           low_memory=False, dtype=str).drop_duplicates()
        room.to_csv('{}{}'.format(new_path, 'room.csv'), index=False)
    except Exception as e:
        print('read e:{}'.format(e))
        room = None

if __name__ == '__main__':
    drop_file_dupicate()