__author__ = 'Abhay Gupta'
__version__ = 0.1

import pyhs2
import time
import MySQLdb
import datetime
import sys
import pytz


def DATE_TIME():
    return datetime.datetime.now(pytz.timezone('Asia/Calcutta'))

def FORMATTED_TIME():
    return datetime.datetime.strptime(str(DATE_TIME()).split('.')[0], '%Y-%m-%d %H:%M:%S')

#Spinner
def DrawSpinner(counter):
    if counter % 4 == 0:
        sys.stdout.write("/")
    elif counter % 4 == 1:
        sys.stdout.write("-")
    elif counter % 4 == 2:
        sys.stdout.write("\\")
    elif counter % 4 == 3:
        sys.stdout.write("|")
    sys.stdout.flush()
    sys.stdout.write('\b')

#Generator
def neighourhood(iterable):
    iterator = iter(iterable)
    prev = None
    item = iterator.next()  # throws StopIteration if empty.
    for next in iterator:
        yield (prev,item,next)
        prev = item
        item = next
    yield (prev,item,None)


#hive cursor
def get_cursor():
        conn = pyhs2.connect(host='',
               port=10000,
               authMechanism="PLAIN",
               user='hadoop',
               password='',
               database='test')
        return conn.cursor()

"""
def get_cursor():
        conn = MySQLdb.connect(user='db', passwd='',
                              host='',
                              db='test')
        return conn.cursor()
"""

def get_mysql_cursor():
        conn = MySQLdb.connect(user='db', passwd='',
                              host='',
                              db='test')
        return conn.cursor()

def get_records():
        cur = get_cursor()
        cur.execute("select * from user_location_history")
        #Fetch table results
        return cur.fetchall()

def get_user_movement():
        #Initializing
        location_dict = {}
        #Fetching all the records
        records = get_records()
        counter = 0
        for record in records:
                counter = counter + 1
                DrawSpinner(counter)
                if location_dict.has_key(record[1]):
                        location_dict[record[1]].append(int(record[3]))
                else:
                        location_dict[record[1]] = [int(record[3])]
        return location_dict

#For performance improvements use list instead of dictionary as we don't need count of the users here
def prepare_movement_data():
        #Initializing
        city_movement_data_dict = {}
        user_location_dict = get_user_movement()
counter = 0
        for user_id, user_movement_path in user_location_dict.iteritems():
                counter = counter + 1
                DrawSpinner(counter)
                if len(set(user_movement_path)) > 1:
                        if city_movement_data_dict.has_key(tuple(user_movement_path)):
                                city_movement_data_dict[tuple(user_movement_path)] = city_movement_data_dict[tuple(user_movement_path)] + 1
                        else:
                                city_movement_data_dict[tuple(user_movement_path)] = 1
        return city_movement_data_dict

def store_mining_results(unique_movement_map_tuple):
        sql_query = None
        insert_flag = False
        update_flag = False
        cur = get_mysql_cursor()
        for prev, current, next in neighourhood(unique_movement_map_tuple):
                #Execute query
                cur.execute('select * from talent_flow_location')
                #Handling the empty table case
                fetched_data = cur.fetchall()
                if len(fetched_data) == 0 and prev is not None and current is not None and prev != current and current != next:
                        sql_query = 'insert into talent_flow_location(location_from, location_to, count)\
                                                values('+  str(prev) + ',' + str(current) + ', 1 )'
                        cur.execute(sql_query)
                else:
                        insert_flag = False
                        update_flag = False
                        for record in fetched_data:
                                if record[2] == prev and record[3] == current:
                                        update_flag = True
                                        sql_query = 'update talent_flow_location set count = ' + str(record[3] + 1) +\
                                        ' where id = ' + str(record[0])
                                        cur.execute(sql_query)
                                elif prev is not None and current is not None and prev != current and current != next:
                                        insert_flag = True
                if update_flag == False and insert_flag == True:
                        #Insert only if the entry doesn't exists
                        #A quick fix. It can be improved later.
                        cur2 = get_mysql_cursor()
                        cur2.execute('select * from talent_flow_location where location_from = ' +\
                                         str(prev) + ' and location_to = ' + str(current))
                        if len(cur2.fetchall()) == 0:
                                sql_query = 'insert into talent_flow_location\
                                                (location_from, location_to, count)\
                                                values('+  str(prev) + ',' + str(current)+', 1)'
                                cur.execute(sql_query)
                        cur2.close()
        cur.close()

if __name__ == '__main__':
        start_time = time.time()
        #spinner = spinning_cursor()
        print 'Preparing data...'
        city_movement_data_dict = prepare_movement_data()
        sys.stdout.flush()
        sys.stdout.write('\b')
        print '\nData prepared for mining.'
        print 'Processing data and storing results...'
        counter = 0
        for unique_movement_map, unique_user_count in city_movement_data_dict.iteritems():
                #print str(unique_movement_map), ' : ', str(unique_user_count), ' user(s) relocated through this path'
                store_mining_results(unique_movement_map)
                counter = counter + 1
                DrawSpinner(counter)
        sys.stdout.flush()
        sys.stdout.write('\b')
        end_time = time.time()
        print '\nData mining complete!'
        print '\nTotal execution time = ', str(end_time - start_time), ' seconds\n'
