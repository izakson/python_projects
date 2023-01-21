## Файл с настройками, параметрами, конфигурациями

import tableauserverclient as TSC
import logging
import sys
import urllib

path = r'C:/reports_sender/'

only_force_start = True


# Подключаем логирование 
def get_logger(name=__file__, file=path + 'log.txt', encoding='utf-8'):
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s] %(filename)s:%(lineno)d %(levelname)-8s %(message)s')
    fh = logging.FileHandler(file, encoding=encoding)
    fh.setFormatter(formatter)
    log.addHandler(fh)
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setFormatter(formatter)
    log.addHandler(sh)
    return log

log = get_logger()
print = log.debug


# Доступ к БД
crm_db_params = urllib.parse.quote_plus('DRIVER={SQL Server};'
                                        'SERVER=server;'
                                        'PORT=1000;'
                                        'DATABASE=db_crm;'
                                        'UID=name;'
                                        'PWD=password')
dwh_db_params = urllib.parse.quote_plus('DRIVER={SQL Server};'
                                        'SERVER=server;'
                                        'PORT=1000;'
                                        'DATABASE=db_dwh;'
                                        'UID=name;'
                                        'PWD=password')

db_dict = {'server.db_crm': crm_db_params,
           'server.db_dwh': dwh_db_params
           }


# Аналитика по метрикам
# Указываем пути к файлам, необходимым для построения отчета
logo = path + 'logo.jpg'
ma_intro_slide_image = path + '/ma_intro_slide.jpg'
# Указываем данные, необходимые для построения summary отчета
ma_summary_header = 'SUMMARY'
ma_summary_list = ['11b065fe-1c8e-4c19-a2e5-f69edd31acf2',
                   'e0c58da6-5b98-4a43-8b76-efe61c3789f7',
                   '3c431443-68d4-450c-bbce-2aa1e33f3e12',
                   '1dfbab5c-5346-4126-9f52-92d9b4a6fb40']
ma_managers_headers = ["ОБЩИЕ ПОКАЗАТЕЛИ ПО МЕНЕДЖЕРАМ", "ОБЩИЕ ПОКАЗАТЕЛИ ПО МЕНЕДЖЕРАМ. ВЫПОЛНЕНИЕ ПЛАНА В РУБЛЯХ, %"]
ma_managers_list = []

# Устанавливаем шрифт и формат листа
cofo_font = 'CoFo'
cofo_font_path = path + '/cofosans.ttf'
legal_width = 89 * 4
legal_hight = 217

# Параметры Tableau сервера
tableau_server_name = 'https://tableau.company.ru'
tableau_server_login = 'login'
tableau_server_password = 'password'
tableau_server_config = {
    'my_env': {
        'server': tableau_server_name,
        'api_version': '3.14',
        'username': tableau_server_login,
        'password': tableau_server_password,
        'site_name': '',
        'site_url': ''
    }
}

# Параметры почтового сервера
email = 'tableau@company.ru'
mail_server_name = 'mail.company.ru'
mail_server_port = 1000
mail_server_login = 'login'
mail_server_password = 'password'

# Путь к файлу с данными для доступа к Google Sheets
credentials_file = path + 'python-sheets-323511-87a9a20a2b90.json'

# Айди Google Sheet с параметрами рассылок
spreadsheet_id = '1dJ2r64MVB72yGDMh5vsWzNTi856Wd3Y9tg5eJQt6PQo'

# Список возможных периоичностей
list_of_periodicity_fields = ['periodicity_type', 'start_date', 'periodicity', 'weekday', 'month', 'monthday', 'time']

# Параметры приложения, бота и чата в Телеграмм
bot_id = 1000000000
bot_username = 'company_reports_sender_bot'
bot_first_name = 'reports_sender'
app_api_id = 10000000
app_api_hash = 'api_hash'
bot_token = '1000000000:token'

# Словарь с данными разных чатов в Телеграмме
chat_id_dict = {'None': None,
                '': None,
                'Отчетность Коммерческого блока для Дирекции по продажам': -10,
                'Summary Москва': -101,
                'Summary SPb': -59,
                'Summary OPT': -67,
                'Тест Summary': -102,
                'Отчёт ЖК1, ЖК2': -1001,
                'Потенциал': -7,
                'Summary по компании Страна': -8,
                'Summary по компании Регионы': -85}

# Описание возможных. форматов выгрузок из Tablealu
# Документация Tableau Server Client https://tableau.github.io/server-client-python/docs/api-ref#pdfrequestoptions-class
orientation_dict = {'': None,
                    'Portrait': TSC.PDFRequestOptions.Orientation.Portrait,
                    'Landscape': TSC.PDFRequestOptions.Orientation.Landscape}

page_type_dict = {'': None,
                  'A3': TSC.PDFRequestOptions.PageType.A3,
                  'A4': TSC.PDFRequestOptions.PageType.A4,
                  'A5': TSC.PDFRequestOptions.PageType.A5,
                  'B4': TSC.PDFRequestOptions.PageType.B4,
                  'Executive': TSC.PDFRequestOptions.PageType.Executive,
                  'Folio': TSC.PDFRequestOptions.PageType.Folio,
                  'Ledger': TSC.PDFRequestOptions.PageType.Ledger,
                  'Legal': TSC.PDFRequestOptions.PageType.Legal,
                  'Letter': TSC.PDFRequestOptions.PageType.Letter,
                  'Note': TSC.PDFRequestOptions.PageType.Note,
                  'Quarto': TSC.PDFRequestOptions.PageType.Quarto,
                  #'Unspecified': TSC.PDFRequestOptions.PageType.Unspecified,
                  'Tabloid': TSC.PDFRequestOptions.PageType.Tabloid}

# Справочники
month_dict = {1: 'января',
              2: 'февраля',
              3: 'марта',
              4: 'апреля',
              5: 'мая',
              6: 'июня',
              7: 'июля',
              8: 'августа',
              9: 'сентября',
              10: 'октября',
              11: 'ноября',
              12: 'декабря'}

month_full_dict = {1: 'Январь',
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

