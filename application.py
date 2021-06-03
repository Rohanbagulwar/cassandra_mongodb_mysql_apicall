#importing all required modules
from flask import Flask, render_template, request, jsonify
import pymongo
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import mysql.connector as cn
import csv
from datetime import datetime
import json,ast
import time

class Logger():
    '''
        It is used save logs into a file

        Parameters
        ----------
        file: log file name Default is logfile.log

        '''
    def __init__(self,file='logfile.log'):
        '''
        init function of logger class
        :param file:
        '''
        self.f_name=file
    def log_operation(self,log_type,log_message):
        '''

        :param log_type:
        :param log_message:
        :return: None
        '''
        now = datetime.now()  # current time
        current_time = now.strftime("%d-%m-%Y %H:%M:%S")
        f = open(self.f_name, "a+")
        f.write(current_time + "," + log_type + "," + log_message + "\n")
        f.close()  # closing log file


apps=Flask(__name__)
#sql code
def sql_connection(database,table,user='root',password='mysql',host='localhost'):
    '''

    :param database:
    :param table:
    :param user:
    :param password:
    :param host:
    :return: connection,user,password,host,database,table
    '''


    connection = cn.connect(
        user=user,
        password=password,
        host=host,
        database=database
    )
    return connection,user,password,host,database,table

def mongo_db_connection(documentry,db_name='abcd'):
    '''

    :param documentry:
    :param db_name:
    :return: client,db_name,documentry
    '''
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    return client,db_name,documentry

def cassandra_connection(column,table,database='trial'):
    '''

    :param column:
    :param table:
    :param database:
    :return: session,column,table,database
    '''

    cloud_config = {
        'secure_connect_bundle': 'F:\secure-connect-trial.zip'
    }
    auth_provider = PlainTextAuthProvider('WKnceQNEdXJZdlQCHSHNWIXt',
                                          '2.YjbPUBb2wA1bP4B9aLyuFs5HKL6ZjIuZj7FoO89l2GZWn6dv1Hk9gky-JsQT3_shhMDjj.mQU,sqxyozO7CH,bKWzgHxdyzx8WU4mGhJN_ZiUf_wDH-cKAcw,x.KZe')
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

    row = session.execute("select release_version from system.local").one()
    print("testing_purpose ",row)
    return session,column,table,database


@apps.route('/sql_create_table',methods=['POST'])
def sql_create_table():
    '''
    post method is used
    :return: json request
    '''
    if(request.method=='POST'):
        try:
            log=Logger('logfile_sql_create.log')
            log.log_operation('info','sql create table api is running')
            user=str(request.json['user'])
            password=str(request.json['password'])
            table=str(request.json['table_nm'])
            host=str(request.json['host'])
            database=str(request.json['db'])
            # print('enter column with datatype'))

            b=str(request.json['column_names_datatypes'])
            log.log_operation('info', f'data fetched succesfully by api for table {table} and dtabase is {database}')
            # print(a,b)
            recive_sql=sql_connection(database,table,user,password,host)
            conn=recive_sql[0]
            log.log_operation('info', f'sql connection is succesful for {table}')
            ex=conn.cursor()
            print(table,b)
            ex.execute(f'create table {table} ({b})')
            log.log_operation('info', f'create table query executed for {database} of {table} ')
            log.log_operation('info', f'{b} created')
            print('database created succesfully')
            conn.commit()
            log.log_operation('info', f'commiting above qury for {database} in {table}')
            conn.close()
            log.log_operation('info', 'connection closed succesfully')
            print('connection close succefully in try block')

        except Exception as a:
            conn.close()
            log.log_operation('Exception', f'exception occurs in databse is {a}')
            log.log_operation('Exception', f'connection close for the {database} in {table}')
            print(str(a))

    return jsonify(f'database created {database} successfylly')

@apps.route('/sql_insert_into_table',methods=['POST'])
def sql_insert_into_table():
    '''
    sql insert table is done by postman api call
    :return: json request
    '''
    if(request.method=='POST'):
        try:
            log=Logger('logfile_sql_insert.log')
            log.log_operation('info','sql insert table is firing')
            recieve=sql_connection('trial','studd_info')
            conn=recieve[0]
            table_name=recieve[-1]
            log.log_operation('info', f'connection is succesful for {recieve[5]} in the table {recieve[-1]}')
            val1=str(request.json['name'])
            val2=str(request.json['surname'])

            # password=str(request.json['password'])
            table=str(request.json['table_nm'])
            # host=str(request.json['host'])
            database=str(request.json['db'])
            log.log_operation('info',f'data fetched succesfully for {database} in the table {table}')

            ex=conn.cursor()
            print(val1,val2)
            ex.execute(f'insert into trial.{table} values ("{val1}","{val2}")')
            log.log_operation('info', f'create table query executed for {database} of table {table} ')

            conn.commit()
            log.log_operation('info', f'commiting above qury for {database} in {table}')
            conn.close()
            log.log_operation('info', 'connection closed succesfully')
            print('connection close succefully in try block')

        except Exception as a:
            conn.close()
            log.log_operation('Exception', f'exception occurs in databse is {a}')
            log.log_operation('Exception', f'connection close for the {database} in {table}')
            print(str(a))

    return jsonify(f'data inserted succesfully for {database} in table {table_name} ')


@apps.route('/sql_delete_into_table', methods=['POST'])
def sql_delete_into_table():
    '''
        sql delete table is done by postman api call
        :return: json request
        '''

    if (request.method == 'POST'):
        try:
            log = Logger('logfile_sql_delete.log')
            log.log_operation('info', 'sql delete from table is firing')
            recieve = sql_connection('trial', 'studd_info')
            conn = recieve[0]
            table_name = recieve[-1]
            log.log_operation('info', f'connection is succesful for {recieve[5]} in the table {recieve[-1]}')

            table = str(request.json['table_nm'])
            # host=str(request.json['host'])
            database = str(request.json['db'])
            attribute=str(request.json['attrib'])
            deleteing_val=str(request.json['delete_values'])
            ex = conn.cursor()
            # print(val1, val2)
            ex.execute(f'delete from {database}.{table} where {attribute}="{deleteing_val}"')
            log.log_operation('info', f'{deleteing_val} data deleted succesfully for {database} in the table {table}')
            log.log_operation('info', f'delete table query executed for {database} of table {table} ')

            conn.commit()
            log.log_operation('info', f'commiting above qury for {database} in {table}')
            conn.close()
            log.log_operation('info', 'connection closed succesfully in try block')
            print('connection close succefully in try block')

        except Exception as a:
            conn.close()
            log.log_operation('Exception', f'exception occurs in databse is {a}')
            log.log_operation('Exception', f'connection close for the {database} in {table}')
            print(str(a))

    return jsonify(f'data deleted succesfully for {database} in table {table_name} ')


@apps.route('/sql_drop_table', methods=['POST'])
def sql_drop_table():
    if (request.method == 'POST'):
        try:
            log = Logger('logfile_sql_drop.log')
            log.log_operation('info', 'sql drop table is firing')
            recieve = sql_connection('trial', 'studd_info')
            conn = recieve[0]
            table_name = recieve[-1]
            log.log_operation('info', f'connection is succesful for {recieve[5]} in the table {recieve[-1]}')
            # password=str(request.json['password'])
            table = str(request.json['table_nm'])
            # host=str(request.json['host'])
            database = str(request.json['db'])


            log.log_operation('info', f'{table} dropped succesfully from {database}')

            ex = conn.cursor()
            # print(val1, val2)
            ex.execute(f'drop table {database}.{table}')
            log.log_operation('info', f'drop table query executed for {database}.{table} ')

            conn.commit()
            log.log_operation('info', f'commiting above qury for {database} in {table}')
            conn.close()
            log.log_operation('info', 'connection closed succesfully')
            print('connection close succefully in try block')

        except Exception as a:
            conn.close()
            log.log_operation('Exception', f'exception occurs in databse is {a}')
            log.log_operation('Exception', f'connection close for the {database} in {table}')
            print(str(a))

    return jsonify(f'Dropped table  succesfully for {database} in table {table_name} ')

@apps.route('/sql_update_into_table', methods=['POST'])
def sql_update_into_table():
    if (request.method == 'POST'):
        try:
            log = Logger('logfile_sql_update.log')
            log.log_operation('info', 'sql update table is firing')
            recieve = sql_connection('trial', 'studd_info')
            conn = recieve[0]
            table_name = recieve[-1]
            log.log_operation('info', f'connection is succesful for {recieve[5]} in the table {recieve[-1]}')

            table = str(request.json['table_nm'])
            # host=str(request.json['host'])
            database = str(request.json['db'])
            setter_value=str(request.json['setting_new__value'])
            to_set=str(request.json['to_set'])
            attrib=str(request.json['attrib_ref'])
            newer_attrib=str(request.json['newer_attrib'])

            log.log_operation('info', f'data fetched succesfully for {database} in the table {table}')

            ex = conn.cursor()
            # print(val1, val2)
            ex.execute(f'update  trial.{table} set {attrib}="{setter_value}" where {newer_attrib}="{to_set}";')
            log.log_operation('info', f'update table query executed for {database} of table {table} ')

            conn.commit()
            log.log_operation('info', f'commiting above qury for {database} in {table}')
            conn.close()
            log.log_operation('info', 'connection closed succesfully')
            print('connection close succefully in try block')

        except Exception as a:
            conn.close()
            log.log_operation('Exception', f'exception occurs in databse is {a}')
            log.log_operation('Exception', f'connection close for the {database} in {table}')
            print(str(a))

    return jsonify(f'data updated succesfully for {database} in table {table_name} ')

@apps.route('/sql_select_into_table', methods=['POST'])
def sql_select_into_table():
    if (request.method == 'POST'):
        try:
            log = Logger('logfile_sql_select.log')
            log.log_operation('info', 'sql select from table is firing')
            recieve = sql_connection('trial', 'studd_info')
            conn = recieve[0]
            table_name = recieve[-1]
            log.log_operation('info', f'connection is succesful for {recieve[5]} in the table {recieve[-1]}')
            table = str(request.json['table_nm'])
            # host=str(request.json['host'])
            database = str(request.json['db'])
            attribute=str(request.json['attrib'])
            limit=int(request.json['limit'])
            ex = conn.cursor()
            # print(val1, val2)
            ex.execute(f'select * from {database}.{table} LIMIT {limit}')
            with open('SQL_select_query_res.txt','w+') as csvfile:
                log.log_operation('info', f'select_query_res.csv succesfully created ')
                res=ex.fetchall()
                res=list(res)
                csv_obj=csv.writer(csvfile)
                csv_obj.writerows(res)
                csvfile.close()
            log.log_operation('info', f'select_query_res.txt succesfully closed ')
            log.log_operation('info', f'select query data fetchedted succesfully for {database} in the table {table}')
            log.log_operation('info', f'select  table query executed for {database} of table {table} ')

            conn.commit()
            log.log_operation('info', f'commiting above qury for {database} in {table}')
            conn.close()
            log.log_operation('info', 'connection closed succesfully in try block')
            print('connection close succefully in try block')

        except Exception as a:
            conn.close()
            csvfile.close()
            log.log_operation('Exception', f'exception occurs in databse is {a}')
            log.log_operation('Exception', f'connection close for the {database} in {table}')
            print(str(a))

    return jsonify(f'data fetched succesfully for {database} in table {table_name} ')
@apps.route('/mongodb_create_table',methods=['POST'])
def mongodb_create_table():
    if(request.method=='POST'):
        try:
            log = Logger('mongodb_create_table_logfile.log')
            db_name = str(request.json['db_nm'])
            documentry_name = str(request.json['documentry_nm'])
            return_conn_tup=mongo_db_connection(db_name,documentry_name)
            log.log_operation('info', 'mongo_db connection succesfully')
            client=return_conn_tup[0]
            log.log_operation('info','data fetched succesfully from the postman api')
            db1=client[db_name]
            log.log_operation('info', f'database made succesfully of {db_name}')
            test=db1.documentry
            log.log_operation('info', f'documentry  made succesfully of {documentry_name}')

            print(db_name,documentry_name)
            rec =(request.json['record'])
            ans=eval(rec)
            print(ans)
            log.log_operation('info', f'data inserted succesfully in{documentry_name} is {ans} in form of json')
            test.insert_one(ans)
            print('database created succesfully')
            client.close()
            print('connection close succefully in try block')
            log.log_operation('info', f'connection  close succesfully in try block')

        except Exception as a:
            client.close()
            log.log_operation('info', f'connection  close succesfully in catch block')
            print('exception'+str(a))
            log.log_operation('Exception', f'exception occurs due to {a} ')

    return jsonify(f'database created {test} sucesfully')

# inventory.update_one(
# {'nm':'Krish'},{'$set':{'qauli':'in'},'$currentDate':{'lastModified':True}})
@apps.route('/mongodb_update_table',methods=['POST'])
def mongodb_update_table():
    if(request.method=='POST'):
        try:
            log = Logger('mongodb_update_table_logfile.log')
            db_name = str(request.json['db_nm'])
            documentry_name = str(request.json['documentry_nm'])
            return_conn_tup=mongo_db_connection(db_name,documentry_name)
            log.log_operation('info', 'mongo_db connection succesfully')
            client=return_conn_tup[0]
            log.log_operation('info','data fetched succesfully from the postman api')
            db1=client[db_name]
            log.log_operation('info', f'database made succesfully of {db_name}')
            test=db1.documentry
            log.log_operation('info', f'documentry  made succesfully of {documentry_name}')
            print(db_name,documentry_name)
            query=(request.json['query'])
            new_value=(request.json['new_value'])
            print('passed value')
            print('passed eval new value')
            ans_q=eval(query)
            ans_v=eval(new_value)
            print(new_value,type(new_value))
            test.update_one(ans_q,ans_v)
            print('update succesful')
            log.log_operation('info', f'data updated succesfully in{documentry_name} is {ans_q,ans_v} in form of json')
            print('database updated succesfully')
            client.close()
            print('connection close succefully in try block')
            log.log_operation('info', f'connection  close succesfully in try block')

        except Exception as a:
            client.close()
            log.log_operation('info', f'connection  close succesfully in catch block')
            print('exception '+str(a))
            log.log_operation('Exception', f'exception occurs due to {a} ')

    return jsonify(f'database updated {test} sucesfully')

@apps.route('/mongodb_drop_table',methods=['POST'])
def mongodb_drop_table():
    if(request.method=='POST'):
        try:
            log = Logger('mongodb_drop_table_logfile.log')
            db_name = str(request.json['db_nm'])
            documentry_name = str(request.json['documentry_nm'])
            return_conn_tup=mongo_db_connection(db_name,documentry_name)
            log.log_operation('info', 'mongo_db connection succesfully')
            client=return_conn_tup[0]
            log.log_operation('info','data fetched succesfully from the postman api')
            db1=client[db_name]
            log.log_operation('info', f'database made succesfully of {db_name}')
            test=db1.documentry
            # log.log_operation('info', f'documentry  made succesfully of {documentry_name}')

            print(db_name,documentry_name)
            test.drop()
            log.log_operation('info','database dropped succesfully')
            print('database dropped succesfully')
            client.close()
            print('connection close succefully in try block')
            log.log_operation('info', f'connection  close succesfully in try block')

        except Exception as a:
            client.close()
            log.log_operation('info', f'connection  close succesfully in catch block')
            print('exception '+str(a))
            log.log_operation('Exception', f'exception occurs due to {a} ')

    return jsonify(f'database dropped {test} sucesfully')




@apps.route('/mongodb_delete_table',methods=['POST'])
def mongodb_delete_table():
    if(request.method=='POST'):
        try:
            log = Logger('mongodb_delete_table_logfile.log')
            db_name = str(request.json['db_nm'])
            documentry_name = str(request.json['documentry_nm'])
            return_conn_tup=mongo_db_connection(db_name,documentry_name)
            log.log_operation('info', 'mongo_db connection succesfully')
            client=return_conn_tup[0]
            log.log_operation('info','data fetched succesfully from the postman api')
            db1=client[db_name]
            log.log_operation('info', f'database table made succesfully of {db_name}')
            test=db1.documentry
            log.log_operation('info', f'documentry  made succesfully of {documentry_name}')
            print(db_name,documentry_name)
            query=(request.json['query'])
            # new_value=(request.json['new_value'])
            ans_q=eval(query)
            # ans_v=eval(new_value)
            # print(new_value,type(new_value))
            test.delete_one(ans_q)
            print('delete succesful')
            log.log_operation('info', f'data deleted succesfully in{documentry_name} is {str(ans_q)} in form of json')
            print('database row deleted succesfully')
            client.close()
            print('connection close succefully in try block')
            log.log_operation('info', f'connection  close succesfully in try block')

        except Exception as a:
            client.close()
            log.log_operation('info', f'connection  close succesfully in catch block')
            print('exception '+str(a))
            log.log_operation('Exception', f'exception occurs due to {a} ')

    return jsonify(f'database row deleted {documentry_name} sucesfully')

@apps.route('/mongodb_select_table',methods=['POST'])
def mongodb_select_table():
    if(request.method=='POST'):
        try:
            log = Logger('mongodb_select_table_logfile.log')
            db_name = str(request.json['db_nm'])
            documentry_name = str(request.json['documentry_nm'])
            return_conn_tup=mongo_db_connection(db_name,documentry_name)
            log.log_operation('info', 'mongo_db connection succesfully')
            client=return_conn_tup[0]
            log.log_operation('info','data fetched succesfully from the postman api')
            db1=client[db_name]
            log.log_operation('info', f'database table made succesfully of {db_name}')
            test=db1.documentry
            log.log_operation('info', f'documentry  made succesfully of {documentry_name}')
            print(db_name,documentry_name)
            limit=int(request.json['limit'])
            ans=test.find().limit(limit)
            with open('mongo_db_select.txt','w+') as f:
                for i in ans:
                    print(i)
                    f.writelines(str(i)+'\n')

                f.close()
            print('succesfully written to file')
            log.log_operation('info', f'data written succesfully in file form of json')
            print('database row fetched succesfully')
            client.close()
            print('connection close succefully in try block')
            log.log_operation('info', f'connection  close succesfully in try block')

        except Exception as a:
            client.close()
            f.close()
            log.log_operation('info', f'connection  close succesfully in catch block')
            print('exception '+str(a))
            log.log_operation('Exception', f'exception occurs due to {a} ')

    return jsonify(f'database row fetched sucesfully from {documentry_name} ')


@apps.route('/cassandra_create_table',methods=['POST'])
def cassandra_create_table():
    if (request.method == 'POST'):
        try:
            cassandra_log=Logger('cassandra_create_table_logfile.log')


            columns = str(request.json['column_names_datatypes'])

            table = str(request.json['table_nm'])
            database = str(request.json['db'])
            cassandra_log.log_operation('info','cassandra data fetched succesfully from api')
            conn_return=cassandra_connection(columns,table,database)
            session=conn_return[0]
            cassandra_log.log_operation('info',f'cassandra connection succesful with cloud cluster to {table}')

            # key=str(request.json['keyspace'])
            row =(f'create table {database}.{table} ({columns});')
            print(row)
            row = session.execute(f"create table  {database}.{table} ( {columns} );")
            cassandra_log.log_operation('info', f'cassandra create table query executed succesfully {table} created in keyspace {database}')

            print(row)
            print('database created succesfully')
            print('connection close succefully in try block')
            cassandra_log.log_operation('info', f'cassandra connection closed sucesfully in try block for {database}.{table}')
        except Exception as a:
            print("exception",str(a))
            cassandra_log.log_operation('Exception', f'Exception happens in the catch block is {str(a)}')

    return 'cassandra create table succesfully fired'

@apps.route('/cassandra_insert_table',methods=['POST'])
def cassandra_insert_table():
    if (request.method == 'POST'):
        try:
            cassandra_log=Logger('cassandra_insert_table_logfile.log')
            # columns = str(request.json['column_names_datatypes'])#flag here
            table = str(request.json['table_nm'])
            database = str(request.json['db'])
            val1=int(request.json['val1'])#id
            val2=str(request.json['val2'])#name
            val3=str(request.json['val3'])#stud_surname
            cassandra_log.log_operation('info','cassandra data fetched succesfully from postman api')
            conn_return=cassandra_connection("columns",table,database)
            session=conn_return[0]
            cassandra_log.log_operation('info',f'cassandra connection succesful with cloud cluster to {table}')
            row = session.execute(f"insert into {database}.{table} (id,name,stud_surname) values({(val1)},'{val2}','{val3}')")
            print(row)
            cassandra_log.log_operation('info', f'cassandra {str(val1),val2,val3} inserted succesfully')
            cassandra_log.log_operation('info', f'cassandra insert table query executed succesfully {table} created in keyspace {database}')

            # print(row)
            print('database created succesfully')
            print('connection close succefully in try block')
            cassandra_log.log_operation('info', f'cassandra connection closed sucesfully in try block for {database}.{table}')
        except Exception as a:
            print("exception",str(a))
            cassandra_log.log_operation('Exception', f'Exception happens in the catch block is {str(a)}')

    return 'cassandra insert table query  succesfully fired'

@apps.route('/cassandra_drop_table',methods=['POST'])
def cassandra_drop_table():
    if (request.method == 'POST'):
        try:
            cassandra_log=Logger('cassandra_drop_table_logfile.log')
            # columns = str(request.json['column_names_datatypes'])#flag here
            table = str(request.json['table_nm'])
            database = str(request.json['db'])
            cassandra_log.log_operation('info','cassandra data fetched succesfully from postman api')
            conn_return=cassandra_connection("columns",table,database)
            session=conn_return[0]
            cassandra_log.log_operation('info',f'cassandra connection succesful with cloud cluster to {table}')
            row = session.execute(f"drop table {database}.{table}")
            print(row)
            cassandra_log.log_operation('info', f'cassandra {table} dropped succesfully')
            cassandra_log.log_operation('info', f'cassandra drop table query executed succesfully {table} created in keyspace {database}')

            # print(row)
            print('database created succesfully')
            print('connection close succefully in try block')
            cassandra_log.log_operation('info', f'cassandra connection closed sucesfully in try block for {database}.{table}')
        except Exception as a:
            print("exception",str(a))
            cassandra_log.log_operation('Exception', f'Exception happens in the catch block is {str(a)}')

    return 'cassandra drop table query  succesfully fired'
@apps.route('/cassandra_delete_table',methods=['POST'])
def cassandra_delete_table():
    if (request.method == 'POST'):
        try:
            cassandra_log=Logger('cassandra_delete_table_logfile.log')
            # columns = str(request.json['column_names_datatypes'])#flag here
            table = str(request.json['table_nm'])
            database = str(request.json['db'])
            val1=str(request.json['val1'])#id
            val2=int(request.json['val2'])#name
            # val3=str(request.json['val3'])#stud_surname
            cassandra_log.log_operation('info','cassandra data fetched succesfully from postman api')
            conn_return=cassandra_connection("columns",table,database)
            session=conn_return[0]
            cassandra_log.log_operation('info',f'cassandra connection sucessful with cloud cluster to {table}')
            row = session.execute(f"DELETE FROM {database}.{table} WHERE {val1}={val2} IF EXISTS")
            print(row)
            cassandra_log.log_operation('info', f'cassandra {table} succesfully deleted row contains {val2}')
            cassandra_log.log_operation('info', f'cassandra delete table query executed succesfully {table} created in keyspace {database}')

            # print(row)
            print('database has row deleted  succesfully')
            print('connection close succefully in try block')
            cassandra_log.log_operation('info', f'cassandra connection closed sucesfully in try block for {database}.{table}')
        except Exception as a:
            print("exception ",str(a))
            cassandra_log.log_operation('Exception', f'Exception happens in the catch block is {str(a)}')

    return 'cassandra delete table query  succesfully fired'

# UPDATE cycling.cyclist_name
# SET comments ='='Rides hard, gets along with others, a real winner'
# WHERE id = fb372533-eb95-4bb4-8685-6ef61e994caa IF EXISTS;
@apps.route('/cassandra_update_table',methods=['POST'])
def cassandra_update_table():
    if (request.method == 'POST'):
        try:
            cassandra_log=Logger('cassandra_update_table_logfile.log')
            # columns = str(request.json['column_names_datatypes'])#flag here
            table = str(request.json['table_nm'])
            database = str(request.json['db'])
            val1=str(request.json['val1'])#id
            new_val=str(request.json['new_val'])
            id=int(request.json['id'])
            # val3=str(request.json['val3'])#stud_surname
            cassandra_log.log_operation('info','cassandra data fetched succesfully from postman api')
            conn_return=cassandra_connection("columns",table,database)
            session=conn_return[0]
            cassandra_log.log_operation('info',f'cassandra connection sucessful with cloud cluster to {table}')
            row = session.execute(f"UPDATE {database}.{table} SET {val1}='{new_val}' WHERE id={id} IF EXISTS")
            print(row)
            cassandra_log.log_operation('info', f'cassandra {table} succesfully updated row contains with {new_val}')
            cassandra_log.log_operation('info', f'cassandra delete table query executed succesfully {table} created in keyspace {database}')

            # print(row)
            print('database has row updated  succesfully')
            print('connection close succefully in try block')
            cassandra_log.log_operation('info', f'cassandra connection closed sucesfully in try block for {database}.{table}')
        except Exception as a:
            print("exception ",str(a))
            cassandra_log.log_operation('Exception', f'Exception happens in the catch block is {str(a)}')

    return 'cassandra update table query succesfully fired'

@apps.route('/cassandra_select_table',methods=['POST'])
def cassandra_select_table():
    if (request.method == 'POST'):
        try:
            cassandra_log=Logger('cassandra_select_table_logfile.log')
            # columns = str(request.json['column_names_datatypes'])#flag here
            table = str(request.json['table_nm'])
            database = str(request.json['db'])
            limit=int(request.json['limit'])#

            cassandra_log.log_operation('info','cassandra data fetched succesfully from postman api')
            conn_return=cassandra_connection("columns",table,database)
            session=conn_return[0]
            cassandra_log.log_operation('info',f'cassandra connection sucessful with cloud cluster to {table}')
            row = session.execute(f"SELECT * FROM {database}.{table}")
            print(row)
            with open('cassandra_select_query.txt','w+') as f:
                for i in row:
                    f.write(str(i)+'\n')
                f.close()


            cassandra_log.log_operation('info', f'cassandra {table} succesfully fetched data from {database}')
            cassandra_log.log_operation('info', f'cassandra select  table query executed succesfully {table} created in keyspace {database}')

            # print(row)
            print('database has fetch data succesfully')
            print('connection close succefully in try block')
            cassandra_log.log_operation('info', f'cassandra connection closed sucesfully in try block for {database}.{table}')
        except Exception as a:
            f.close()
            print("exception ",str(a))
            cassandra_log.log_operation('Exception', f'Exception happens in the catch block is {str(a)}')

    return 'cassandra select table query succesfully fired'

if __name__=='__main__':
    apps.run()






































































