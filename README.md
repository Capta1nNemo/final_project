
## Итоговый проект по программе "Python для продвинутых специалистов" (Мамонов Артем)
# Окружение:
Виртуальная машина Oracle VB, linux ubuntu 22.04.2 LTS
Установлены apache airflow, python 3.10, необходимые библиотеки для python.

#Конфигурация, логгирирование:
Файлы conf_task1.ini, conf_task2.ini, conf_task3.ini содержат конфигурации для заданий.
Файлы task_1.log, task_2.log, task_3.log, task_4.log содержат логи этапов выгрузки данных в таблицы. Уровень логирования INFO. В задании 2 пропущены логи ошибок из-за большого объема файла .log (однотипные ошибки sqlite). Файл airflow_log.log - лог airflow для информирования о создании графа.

Итоговый DAG содержит 5 вершин/задач (вершина 2 имеет степень 2 и разделяется на вершину 3 и вершину 4 - далее ребра сходятся в вершине 5):
1. Создание базы данных и таблиц (okved_table, telecom_companies, vacanciesAPI, common_vacancies, top_skills)
2. Выгрузка ОКВЭД в таблицу okved_table
3. Выгрузка компаний из архива ЕГРЮЛ с кодом ОКВЭД 61 в таблицу telecom_companies
4. Выгрузка вакансий python middle developer с сайта hh.ru с помощью API в таблицу vacanciesAPI
5. Сравнение компаний в таблицах telecom_companies и vacanciesAPI, поиск совпадающих компаний и выгрузка вакансий данных компаний в таблицу common_vacancies, выгрузка 10 самых популярных ключевых навыков в таблицу top_skills с количеством вхождений.

Основной файл для запуска main_dag.py.
Файл okved_2.json содержит файлы для выгрузки кодов ОКВЭД в задаче 2.
Файл final_project.db - готовая база данных по итогу выполнения DAG в airflow.

