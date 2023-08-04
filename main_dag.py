import sqlite3 as sql
import json as j
import configparser as conf
import logging
import time as t
from zipfile import ZipFile
import orjson as oj
from bs4 import BeautifulSoup as bs
import asyncio
import lxml
import fake_useragent as fake
import requests as r
import collections as col
from datetime import timedelta as td
import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

    
def_args = {
    'owner': 'MamonovAV',
    'retires': 5,
    'retry_delay': td(minutes = 5)
    }

air_log = logging.getLogger('airflow_log')
air_log.setLevel(logging.INFO)
air_handler = logging.FileHandler('airflow_log.log', mode = 'w')
air_formatter = logging.Formatter('%(name)s %(asctime)s %(message)s')
air_handler.setFormatter(air_formatter)
air_log.addHandler(air_handler)
air_log.info('dag was created')

@dag(dag_id = 'Final_project_main',
    default_args = def_args,
    description = 'project airflow dag',
    start_date = dt.datetime(2023, 8, 1),
    schedule = None)

def airflow_dag():

    @task()
    def create_tables():
        connect = sql.connect('final_project.db')
        curs = connect.cursor()
        curs.execute('CREATE TABLE okved_table(code TEXT, parent_code TEXT, section TEXT, name TEXT, comment TEXT)')
        curs.execute('CREATE TABLE telecom_companies(КОД_ОКВЭД TEXT, ОГРН INT, ИНН INT, НАЗВАНИЕ TEXT, ПОЛНОЕ_НАЗВАНИЕ TEXT)')
        curs.execute('CREATE TABLE vacanciesAPI (company_name TEXT, position TEXT, job_description TEXT, key_skills TEXT)')
        curs.execute('CREATE TABLE common_vacancies (company_name TEXT, position TEXT, job_description TEXT, key_skills TEXT)')
        curs.execute('CREATE TABLE top_skills (skill TEXT, amount INT)')
        connect.commit()
        connect.close()
        
    @task()
    def task1():
        task1_logger = logging.getLogger('task1')
        task1_logger.setLevel(logging.INFO)
        task1_handler = logging.FileHandler('task_1.log', mode = 'w')
        task1_formatter = logging.Formatter('%(name)s %(asctime)s %(message)s')
        task1_handler.setFormatter(task1_formatter)
        task1_logger.addHandler(task1_handler)


        timecheck = t.time()
        path = conf.ConfigParser()
        path.read('airflow/dags/conf_task1.ini')

        task1_logger.info('START PROJECT')
        connect = sql.connect("final_project.db")
        curs = connect.cursor()
        task1_logger.info('start okved unpacking')
        with open(path['Access']['path'], 'rb') as f:
            file = j.load(f)
            for i in file:
                res = list(i.values())
                try:
                    curs.execute("INSERT INTO okved_table VALUES(?,?,?,?,?);", (res))
                    task1_logger.info('okved added')
                except Exception as ex:
                    task1_logger.error(f'{ex.args}, {ex.__class__}')
                    pass
        connect.commit()
        connect.close()
        task1_logger.info('end okved unpacking')

        time = str(td(seconds = (t.time() - timecheck)))
        task1_logger.info(f"Time: {time[:time.find('.')]}")

    @task()
    def task2():
        task2_logger = logging.getLogger('task2')
        task2_logger.setLevel(logging.INFO)
        task2_handler = logging.FileHandler('task_2.log', mode = 'w')
        task2_formatter = logging.Formatter('%(name)s %(asctime)s %(message)s')
        task2_handler.setFormatter(task2_formatter)
        task2_logger.addHandler(task2_handler)

        timecheck = t.time()
        connect = sql.connect("final_project.db")
        curs, okved_list = connect.cursor(), []

        path = conf.ConfigParser()
        path.read('airflow/dags/conf_task2.ini')
        task2_logger.info('start egrul sorting')
        with ZipFile(path['Access']['path'], 'r') as egrul_zip:
            for i in sorted(egrul_zip.namelist()):
                with egrul_zip.open(i, 'r') as file:
                    zip_read = egrul_zip.read(i).decode(encoding = 'utf-8')
                    json_file = oj.loads(zip_read)
                    for strings in json_file:
                        for i, j in strings['data'].items():
                            if i == 'СвОКВЭД':
                                for x, y in j.items():
                                    if x == 'СвОКВЭДОсн' and type(y) == dict:
                                        for x, y in y.items():
                                            if x == 'КодОКВЭД':
                                                if y.startswith('61') == True:
                                                    okved_list.append(y)
                                                    for i, j in strings.items():
                                                        if i != 'data' and i != 'data_version' and i != 'kpp':
                                                            okved_list.append(j)
                                                try:
                                                    curs.execute("INSERT INTO telecom_companies VALUES(?,?,?,?,?);", (okved_list))
                                                    task2_logger.info(f"{', '.join(okved_list)} - added")
                                                    connect.commit()
                                                except sql.ProgrammingError as error:
                                                    #task2_logger.error(f'{error.args}, {error.__class__}') ---> too large log file
                                                    pass
                                                finally:
                                                    okved_list = []
                    file.close()
        connect.close()
        task2_logger.info('end egrul sorting')

        time = str(td(seconds = (t.time() - timecheck)))
        task2_logger.info(f"Time: {time[:time.find('.')]}")

    @task()
    def task3():
        global page_num
        global key_skills
        task3_logger = logging.getLogger('task3')
        task3_logger.setLevel(logging.INFO)
        task3_handler = logging.FileHandler('task_3.log', mode = 'w')
        task3_formatter = logging.Formatter('%(name)s %(asctime)s %(message)s')
        task3_handler.setFormatter(task3_formatter)
        task3_logger.addHandler(task3_handler)
      
        task3_logger.info('Start pars with API')
        connect = sql.connect('final_project.db')
        curs = connect.cursor()
        conf_url_API = conf.ConfigParser()
        conf_url_API.read('airflow/dags/conf_task3.ini')
        counter = 0
        headers = {'user-agent': fake.UserAgent().random}
        key_skills, page_num, page_counter = [], 0, 0
        timecheck = t.time()
        while counter <= 100:
            
            params = {
                'text': 'middle python developer',
                'search_field': 'name',
                'per_page': '100',
                'page': '{}'.format(page_num),
                'areas': [{'id': '113'}]}
            response = r.get(conf_url_API['Access']['url_API'], headers = headers, params = params)
            if response.status_code == 200:
                try:
                    for i in response.json()['items']:
                        response = r.get(i['url'])
                        resp_json = r.get(i['url']).json()
                        t.sleep(1)
                        key_chars = {'name', 'employer', 'key_skills', 'description'}
                        if key_chars.issubset(set(resp_json.keys())) == True:
                            async def vacancy_name():
                                global vac_name
                                vac_name = resp_json['name']
                                return vac_name
                                await asyncio.sleep(0.5)
                            async def company_name():
                                global comp_name
                                comp_name = resp_json['employer']['name']
                                return comp_name
                                await asyncio.sleep(0.5)
                            async def keys():
                                global key_skills
                                for i in resp_json['key_skills']:
                                    key_skills.append(*i.values())
                                return key_skills
                                await asyncio.sleep(0.5)
                            key_skills = []
                            async def descr_fin():
                                global description
                                descr = resp_json['description']
                                description = bs(descr, 'lxml').text
                                return description
                                await asyncio.sleep(0.5)
                            async def trial():
                                global counter
                                global page_counter
                            
                                task_1 = asyncio.create_task(vacancy_name())
                                task_2 = asyncio.create_task(company_name())
                                task_3 = asyncio.create_task(keys())
                                task_4 = asyncio.create_task(descr_fin())

                                await task_1
                                await task_2
                                await task_3
                                await task_4
                            asyncio.run(trial())
                            if vac_name != '' and comp_name != '' and  description !=  '' and key_skills != []:
                                temp = curs.execute('SELECT * FROM vacanciesAPI WHERE job_description=?', (description, )).fetchone()
                                if temp == None:
                                    curs.execute('INSERT INTO vacanciesAPI VALUES(?,?,?,?)', (comp_name, vac_name, description, ', '.join(key_skills)))
                                    counter += 1
                                    task3_logger.info(f'added: {comp_name}, {vac_name}')
                                    connect.commit()        
                                else:
                                    pass
                                page_counter += 1
                                if page_counter % 100 == 0 and page_counter != 0:
                                    page_num += 1
                  
                except Exception as ex:
                    task3_logger.error(f'{ex.args}, {ex.__class__}')
                    t.sleep(2)
                    pass                
            else:
                pass
        connect.close()
        task3_logger.info(f'Количество найденных вакансий: {counter}')
        task3_logger.info('End pars with API')

        time = str(td(seconds = (t.time() - timecheck)))
        task3_logger.info(f"Time: {time[:time.find('.')]}")

    @task()
    def task4():
        timecheck = t.time()
        comp_name_list = set()
        vacancy_check = dict()

        task4_logger = logging.getLogger('task4')
        task4_logger.setLevel(logging.INFO)

        task4_handler = logging.FileHandler('task_4.log', mode = 'w')
        task4_formatter = logging.Formatter('%(name)s %(asctime)s %(message)s')

        task4_handler.setFormatter(task4_formatter)
        task4_logger.addHandler(task4_handler)

        connect = sql.connect("final_project.db")
        curs = connect.cursor()
        curs.execute('''SELECT ПОЛНОЕ_НАЗВАНИЕ FROM telecom_companies''')

        common_skills = []

        for i in curs.fetchall():
            for j in i:
                comp_name = j[j.find('"')+1: j.rfind('"')].strip().replace('"', '')
                if comp_name != '':
                    comp_name_list.add(comp_name)

        curs = connect.cursor()
        curs.execute('SELECT * FROM vacanciesAPI')
        task4_logger.info('Adding common companies into table common_vacancies:')
        for j in curs.fetchall():
            for i in comp_name_list:
                if j[0].upper() == i or j[0].upper() in i.split() or i in j[0].upper().split():
                    temp = curs.execute('SELECT * FROM common_vacancies WHERE job_description=?', (j[2], )).fetchone()
                    if temp == None:
                        curs.execute('INSERT INTO common_vacancies VALUES(?,?,?,?);', j)
                        task4_logger.info(f'added: {j[0]}')
                        connect.commit()
                        for k in j[3].split(', '):
                            common_skills.append(k)
        task4_logger.info('Top 10 skills and adding top skills into table top_skills:')
        for i in col.Counter(common_skills).most_common()[:10]:
            curs.execute('INSERT INTO top_skills VALUES(?,?);', i)
            connect.commit()
            task4_logger.info(f'{i[0]} - {i[1]} vacancies')
            task4_logger.info(f'added: {i[0]}')
           
        connect.close()                
        time = str(td(seconds = (t.time() - timecheck)))
        task4_logger.info(f"Time: {time[:time.find('.')]}")
        
        task4_logger.info('STOP PROJECT')

    create_tables() >> task1() >> [task2(), task3()] >> task4()

    
greet_dag = airflow_dag()
