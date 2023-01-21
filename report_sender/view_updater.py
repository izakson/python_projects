# Скрипт для обновления файла со списком всех дашбордов на сервере (для ручного запуска при необходимости)

import tableauserverclient as TSC
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying.workbooks import get_views_dataframe
import pandas as pd
import datetime
import os
from PyPDF2 import PdfFileMerger
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import apiclient
from shutil import copyfile
from pyrogram import Client

import config


def update_views(path=''):
    # Обновляет/создает файл "views" со списком всех дашбордов на сервере Tableau и сохраняет его в указанную
    # директорию (по умолчанию в текущую).
    conn = TableauServerConnection(config.tableau_server_config, env='my_env')
    conn.sign_in()
    views_df = pd.DataFrame(get_views_dataframe(conn))
    print(views_df)
    views_df.to_excel(r'C:\reports_sender\new_views.xlsx')



update_views(path='')
