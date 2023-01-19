# Файл конфигураций и справочных материалов


import sqlalchemy
import urllib
from atlassian import Jira
import pyodbc

is_test = False

# -----------------------------------------------------------------------------------------------
# --------------------------------------СПРАВОЧНЫЕ МАТЕРИАЛЫ-------------------------------------
# -----------------------------------------------------------------------------------------------
month_dict = {1: 'Январь',
              2: 'Февраль',
              3: 'Март',
              4: 'Апрель',
              5: 'Май',
              6: 'Июнь',
              7: 'Июль',
              8: 'Август',
              9: 'Сентябрь',
              10: 'Октябрь',
              11: 'Ноябрь',
              12: 'Декабрь'}

# -----------------------------------------------------------------------------------------------
# -------------------------------------------МЕТОДОЛОГИЯ-----------------------------------------
# -----------------------------------------------------------------------------------------------

from_year = 2022  # Нижняя граница (год) временных рамок анализа

list_of_metrics_convergence = ['lines', 'qty', 'sqr', 'rub', 'compensation', 'cost_num', 'cost_denom']  # Список показателей сходимости
list_of_metrics_clarity = ['doubles', 'wrong_qty', 'wrong_sqr',
                           'wrong_rub', 'wrong_compensation', 'wrong_cost_num', 'wrong_cost_denom']  # Список показателей чистоты
list_of_metrics_permanence = ['is_gone', 'is_new_date', 'changed_date', 'changed_doubles', 'changed_qty', 'changed_sqr', 'changed_rub', 'changed_cost_num', 'changed_cost_denom', 'changed_compensation']  # Список показателей сходимости

list_of_metrics = list_of_metrics_convergence + list_of_metrics_clarity  # Список показателей для статистики

max_sqr_corrected = 5  # Максимальное количество метров, на которое разрешается проводить операции корректировки

ids = ['cid', 'oid', 'operation']  # Список полей, по которым определяем уникальные продажи и возвраты
# (cid = номер договора, oid = номер объекта недвижимости, operation = операция)
ids_corrections = ['cid', 'oid', 'operation', 'sqr', 'rub']  # Список полей, по которым определяем уникальные
# корректировки (также учитываем площадь и сумму выручки)
ids_compensations_corrections = ['cid', 'oid', 'operation', 'compensation']  # Список полей, по которым определяем
# уникальные корректировки компенсаций (также учитываем сумму компенсации)

columns_log = ['cid', 'oid', 'operation', 'date', 'doubles', 'qty', 'sqr', 'rub', 'compensation', 'cost_num', 'cost_denom']  # Список полей для логов

# -----------------------------------------------------------------------------------------------
# --------------------------------------ПОДКЛЮЧЕНИЕ К БД-----------------------------------------
# -----------------------------------------------------------------------------------------------

# Данные пользователя
username = "user"
password = "password"
driver = "ODBC Driver 13 for SQL Server"

# Параметры подключения к базе для выгрузки результатов
bigdatatest_server = "server"
bigdatatest_reporting_db = "Reporting_KB"

bigdatatest_engine = sqlalchemy.create_engine(
    'mssql+pyodbc://'+username+':'+password+'@'+bigdatatest_server+'/'+bigdatatest_reporting_db+'?driver='+driver, fast_executemany=True)
bigdatatest_conn = bigdatatest_engine.connect()

# Параметры подключения к DWH (DWH2CRM)
dwh_server = "dwh"
dwh_port = 1000
dwh_DWH2CRM_db = "DWH2CRM"

dwh_db_params = urllib.parse.quote_plus('DRIVER={'+driver+'};'
                                        'SERVER='+dwh_server+';'
                                        'PORT='+str(dwh_port)+';'
                                        'DATABASE='+dwh_DWH2CRM_db+';'
                                        'UID='+username+';'
                                        'PWD='+password)
dwh_engine = sqlalchemy.create_engine(
    "mssql+pyodbc:///?odbc_connect={}".format(dwh_db_params), fast_executemany=True)

# Параметры подключения к CRM1 (ReportCRM)
crm1_server = "crm"
crm1_port = 1000
crm1_datamarts_db = "ReportCRM"

crm1_db_params = urllib.parse.quote_plus('DRIVER={'+driver+'};'
                                        'SERVER='+crm1_server+';'
                                        'PORT='+str(crm1_port)+';'
                                        'DATABASE='+crm1_datamarts_db+';'
                                        'UID='+username+';'
                                        'PWD='+password)
crm1_engine = sqlalchemy.create_engine(
    "mssql+pyodbc:///?odbc_connect={}".format(crm1_db_params), fast_executemany=True)

# Параметры подключения к CRM2 (ReportCRM20)
crm2_server = "crm2"
crm2_port = 1000
crm2_dwh2crm_db = "ReportCRM20"

crm2_db_params = urllib.parse.quote_plus('DRIVER={'+driver+'};'
                                        'SERVER='+crm2_server+';'
                                        'PORT='+str(crm2_port)+';'
                                        'DATABASE='+crm2_dwh2crm_db+';'
                                        'UID='+username+';'
                                        'PWD='+password)
crm2_engine = sqlalchemy.create_engine(
    "mssql+pyodbc:///?odbc_connect={}".format(crm2_db_params), fast_executemany=True)

# -----------------------------------------------------------------------------------------------
# -----------------------------------------SQL-запросы-------------------------------------------
# -----------------------------------------------------------------------------------------------

# SQL-запрос для выгрузки продаж DWH (dwh2crm / [tableau].[Продажи_Показатели])
dwh_sales_sql = "select [Дата] as date," \
                "[Номер договора] as cid," \
                "[Помещение] as oid," \
                "[Тип операции] as operation," \
                "[Объем продаж, шт] as qty," \
                "[Объем продаж, м2] as sqr," \
                "[Выручка, руб] as rub," \
                "[Компенсация, руб] as compensation," \
                "[Площадь для расчета цены за м2] as cost_denom," \
                "[Стоимость для расчета цены за м2] as cost_num" \
                " from [tableau].[Продажи_Показатели] a" \
                " left join [tableau].[Объект недвижимости] c " \
                " on a.tisa_ArticleId=c.tisa_ArticleId" \
                " left join [tableau].[Договор] d" \
                " on a.Opportunityid=d.Opportunityid" \
                " left join  [tableau].[Проект] e" \
                " on a.ProjectId=e.Project_key" \
                " where ([Особый Стр.резерв] is null or [Особый Стр.резерв] not in ('Спец. продажа'))" \
                " and [Группа договоров] in ('ДДУ','ПДДУ','ДКП','ПДКП','ДУПТ')" \
                " and [Дата]>='20220101' and [Дата]<='20221206'"

crm1_sales_sql = "select s.date_ AS date," \
                 " OP.OP_Name as cid, " \
                 "a.Помещение as oid, " \
                 "s.OperTypeName as operation," \
                 "s.Qty as qty, " \
                 "s.tisa_Space AS sqr, " \
                 "s.tisa_Sum as rub, " \
                 "s.tisa_CompensationSumm AS compensation," \
                 "s.[1kvm_space] as cost_denom," \
                 "s.[1kvm_summ] as cost_num" \
                 " from [dbo].[fnGet_Sales]('"+str(from_year)+"0101', '20221206',2, 0) S" \
                 " join dim_Article a ON S.tisa_ArticleId = a.ПомещениеID" \
                 " JOIN V_Opportunity op on s.OpportunityId = OP.OpportunityId"

crm2_sales_sql = "select s.date_ AS date," \
                 " OP.OP_Name as cid, " \
                 "a.Помещение as oid, " \
                 "s.OperTypeName as operation," \
                 "s.Qty as qty, " \
                 "s.tisa_Space AS sqr, " \
                 "s.tisa_Sum as rub, " \
                 "s.tisa_CompensationSumm AS compensation," \
                 "s.[1kvm_space] as cost_denom," \
                 "s.[1kvm_summ] as cost_num" \
                 " from ReportCRM20.material.[fnGet_Sales]('"+str(from_year)+"0101', '20221206', 0) S" \
                 " join map.dim_article a ON S.tisa_ArticleId = a.ПомещениеID" \
                 " JOIN map.V_Opportunity op on s.OpportunityId = OP.OpportunityId"

# -----------------------------------------------------------------------------------------------
# ------------------------------------------РАССЫЛКА---------------------------------------------
# -----------------------------------------------------------------------------------------------
convergence_errors_intro_dict = {'is_absent': 'Расхождения количества записей',
                     'descr_qty': 'Расхождения в количестве продаж',
                     'descr_sqr': 'Расхождения в объеме продаж',
                     'descr_rub': 'Расхождения в сумме выручки от продаж',
                     'descr_compensation': 'Расхождения в сумме компенсации',
                     'descr_cost_num': 'Расхождения в числителе цены',
                     'descr_cost_denom': 'Расхождения в знаменателе цены'
                                 }

clarity_errors_intro_dict = {'doubles': 'Дубли',
                     'wrong_qty': 'Аномальные значения количества',
                     'wrong_sqr': 'Аномальные значения площади',
                     'wrong_rub': 'Аномальные значения выручки',
                     'wrong_compensation': 'Аномальные значения компенсации',
                     'wrong_cost_num': 'Аномальные значения числителя цены',
                     'wrong_cost_denom': 'Аномальные значения знаменателя цены'
                             }

changes_intro_dict = {'is_gone': 'Пропажи записей',
                      'is_new_date': 'Новые записи в закрытых периодах',
                      'changed_date': 'Изменения отчетного периода',
                      'changed_doubles': 'Появления дублей',
                      'changed_qty': 'Изменения в количестве',
                      'changed_sqr': 'Изменения в объеме',
                      'changed_rub': 'Изменения в сумме выручки',
                      'changed_compensation': 'Изменения в сумме компенсации',
                      'changed_cost_num': 'Изменения в числителе цены',
                      'changed_cost_denom': 'Изменения в знаментаеле цены'
                      }

list_of_new_wrongs_qty = []
list_of_new_wrongs_sqr = []
list_of_new_wrongs_rub = []
list_of_new_wrongs_compensation = []
list_of_new_wrongs_cost_num = []
list_of_new_wrongs_cost_denom = []
list_of_new_descr_qty = []
list_of_new_descr_sqr = []
list_of_new_descr_rub = []
list_of_new_descr_compensation = []
list_of_new_descr_cost_num = []
list_of_new_descr_cost_denom = []
list_of_new_absent = []
list_of_new_doubles = []

jira_texts_dict = {'is_absent': " ".join(list_of_new_absent),
                     'descr_qty': " ".join(list_of_new_descr_qty),
                     'descr_sqr': " ".join(list_of_new_descr_sqr),
                     'descr_rub': " ".join(list_of_new_descr_rub),
                     'descr_compensation': " ".join(list_of_new_descr_compensation),
                    'descr_cost_num': " ".join(list_of_new_descr_rub),
                     'descr_cost_denom': " ".join(list_of_new_descr_compensation),
                     'doubles': " ".join(list_of_new_doubles),
                     'wrong_qty': " ".join(list_of_new_wrongs_qty),
                     'wrong_sqr': " ".join(list_of_new_wrongs_sqr),
                     'wrong_rub': " ".join(list_of_new_wrongs_rub),
                     'wrong_compensation': " ".join(list_of_new_wrongs_compensation),
                   'wrong_cost_num': " ".join(list_of_new_descr_cost_num),
                   'wrong_cost_denom': " ".join(list_of_new_descr_cost_denom)
                   }


# -----------------------------------------------------------------------------------------------
# ------------------------------------ОПОВЕЩЕНИЯ И СБОР СТАТИСТИКИ-------------------------------
# -----------------------------------------------------------------------------------------------
# Параметры почтового сервера
email = 'email@email.ru'
mail_server_name = 'mail.company.ru'
mail_server_port = 100
mail_server_login = 'login'
mail_server_password = 'password'


# Список получателей оповещения об ошибках на почту
in_charge = ['user@company.ru']
recipients = ['user@company.ru']

# Пути к файлам с выгрузкой анализа
if not is_test:
    path = r'C:/data_check/sales/'
else:
    path = r'C:/data_check/test/sales/'
sales_analysis_xlsx = path + 'Выгрузка_продаж.xlsx'
is_absent_xlsx = path + 'Отсутствующие_записи.xlsx'
descr_qty_xlsx = path + 'Расхождения_в_количестве.xlsx'
descr_sqr_xlsx = path + 'Расхождения_в_площади.xlsx'
descr_rub_xlsx = path + 'Расхождения_в_выручке.xlsx'
descr_compensation_xlsx = path + 'Расхождения_в_компенсации.xlsx'
descr_cost_num_xlsx = path + 'Расхождения_в_числителе_цены.xlsx'
descr_cost_denom_xlsx = path + 'Расхождения_в_знаменателе_цены.xlsx'


statistics_data = {'source_name': [],
                   'rep_period': [],
                   'source_name_compare': [],
                   'metrics': [],
                   'value': [],
                   'value_compare': [],
                   'lines': []
                   }  # Данные для статистики

# Данные для авторизации в Jira
jira = Jira(
     url="https://hd.company.ru/",
     username='user',
     password='password')

jira_project_key = 'TB'
jira_labels = ['Качество_Данных', 'BUG']

# -----------------------------------------------------------------------------------------------
# -----------------------------------------ТИПЫ ДАННЫХ-------------------------------------------
# -----------------------------------------------------------------------------------------------

sales_analysis_dtype = dict.fromkeys(ids, sqlalchemy.types.NVARCHAR(length=100))  # Словарь для типов

statistics_dtype = {'date': sqlalchemy.DateTime(),
                    'source_name': sqlalchemy.types.NVARCHAR(100),
                    'rep_period': sqlalchemy.DateTime(),
                    'source_name_compare': sqlalchemy.types.NVARCHAR(100),
                    'metrics': sqlalchemy.types.NVARCHAR(100),
                    'value': sqlalchemy.types.Float,
                    'value_compare': sqlalchemy.types.Float,
                    'lines': sqlalchemy.types.Integer}

log_dtype = {'cid': sqlalchemy.types.NVARCHAR(100),
             'oid': sqlalchemy.types.NVARCHAR(100),
             'operation': sqlalchemy.types.NVARCHAR(100),
             'date': sqlalchemy.DateTime(),
             'doubles': sqlalchemy.types.Integer,
             'qty': sqlalchemy.types.Integer,
             'sqr': sqlalchemy.types.Float,
             'rub': sqlalchemy.types.Float,
             'compensation': sqlalchemy.types.Float,
             'cost_num': sqlalchemy.types.Float,
             'cost_denom': sqlalchemy.types.Float,
             'datetime': sqlalchemy.DateTime()
             }

logs_changes_dtype = {'cid': sqlalchemy.types.NVARCHAR(100),
                      'oid': sqlalchemy.types.NVARCHAR(100),
                      'operation': sqlalchemy.types.NVARCHAR(100),
                      'date_old': sqlalchemy.DateTime(),
                      'doubles_old': sqlalchemy.types.Integer,
                      'qty_old': sqlalchemy.types.Integer,
                      'sqr_old': sqlalchemy.types.Float,
                      'rub_old': sqlalchemy.types.Float,
                      'compensation_old': sqlalchemy.types.Float,
                      'cost_num_old': sqlalchemy.types.Float,
                      'cost_denom_old': sqlalchemy.types.Float,
                      'is_gone': sqlalchemy.types.Boolean,
                      'is_new_date': sqlalchemy.DateTime(),
                      'changed_date': sqlalchemy.DateTime(),
                      'changed_doubles': sqlalchemy.types.Integer,
                      'changed_qty': sqlalchemy.types.Integer,
                      'changed_sqr': sqlalchemy.types.Float,
                      'changed_rub': sqlalchemy.types.Float,
                      'changed_compensation': sqlalchemy.types.Float
                      ,'changed_cost_num': sqlalchemy.types.Float,
                      'changed_cost_denom': sqlalchemy.types.Float
                      }

initial_logs_changes_dtype = logs_changes_dtype
