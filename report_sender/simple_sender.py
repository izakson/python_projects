# Файл с основными функциями для скачивания, форматирования и рассылки простых отчетов

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
    views_df.to_excel(path + "/views.xlsx")

# Подключаемся к Tableau сервер с помощью библиотеки TSC
tableau_auth = TSC.TableauAuth(config.tableau_server_login, config.tableau_server_password)
server = TSC.Server(config.tableau_server_name)
server.use_server_version()


def check_update(view_id, hours):
    # Функция принимает на вход id листа и количество часов. Проверяет, был ли обновлен воркбук,
    # которому принадлежит лист, не ранее чем за указанное количество часов.
    # Возвращает true, если воркбук был обновлен вовремя и его можно скачивать.
    if hours > 0:
        config.print('Проверяю, что воркбук обновлен за ' + str(hours) + ' ч.')
        with server.auth.sign_in(tableau_auth):
            view = server.views.get_by_id(view_id)
            workbook = server.workbooks.get_by_id(view.workbook_id)
            workbook_update_time = workbook.updated_at.replace(tzinfo=None) + datetime.timedelta(hours=3)
            diff = datetime.datetime.today() - workbook_update_time
            if diff.seconds / 3600 <= hours:
                config.print('Воркбук обновлен вовремя.')
                return True
            else:
                config.print('Воркбук НЕ обновлен вовремя.')
                return False
    return True

def format_date(string):
    # Приводит даты к единому формату
    if str(string).count(".") == 2:
        string = pd.to_datetime(string, format='%d.%m.%Y')
    return string


def download_view(view_id,
                  view_number,
                  report_name,
                  projects_list=[''],
                  project_filter_name=[''],
                  page_type='',
                  orientation='',
                  path='',
                  list_of_filters=[''],
                  list_of_values=[''],
                  project_first=False,
                  apply_project_list=[''],
                  download_format='pdf',
                  png_resolution=None,
                  max_age=-1):
    # Функция принимает на вход id листа, порядковый номер листа, название отчета, список проектов и
    # название фильтра с выбором проекта (если нужно скачать лист несколько раз с перебором проектов),
    # тип листа, ориентацию, список фильтров и список значений. Параметр project_first определяет, какой перебор
    # приоритетнее для отчета - когда True - по проектам, False - по слайдам.
    # Скачивает указанный лист в нужной ориентации
    # и с примененными фильтрами в формате pdf или png (регулируется параметром download_format,
    # называет его в формате "порядковый номер листа (+ порядковый
    # номер проекта, если есть), название отчета (+ название проекта, если есть),
    # сохраняет в отдельную папку с названием отчета.
    config.print('Скачиваю лист № ' + str(view_number))

    with server.auth.sign_in(tableau_auth):
        view = server.views.get_by_id(view_id)
        if download_format == 'pdf':
            req_option = TSC.PDFRequestOptions(orientation=config.orientation_dict[orientation],
                                               page_type=config.page_type_dict[page_type],
                                               maxage=int(max_age))

        elif download_format == 'png':
            if png_resolution == 'High':
                req_option = TSC.ImageRequestOptions(imageresolution=TSC.ImageRequestOptions.Resolution.High,maxage=int(max_age))
            else:
                req_option = TSC.ImageRequestOptions(maxage=int(max_age))

        if str(view_number) in apply_project_list:
            for j in range(len(projects_list)):
                if projects_list[j] != '' or j == 0:
                    if projects_list[j] != '':
                        projects_list_list = str(projects_list[j]).split(',')
                        projects_list_list = [x for x in projects_list_list if x != 'None' and x != '']
                        for k in range(len(project_filter_name)):
                            req_option.vf(project_filter_name[k], projects_list_list[k])
                            config.print('Применен фильтр: ' + project_filter_name[k] + ' : ' + projects_list_list[k])
                    for i in range(len(list_of_filters)):
                        if list_of_filters[i] != '':
                            list_of_values[i] = format_date(list_of_values[i])
                            req_option.vf(list_of_filters[i], list_of_values[i])
                            config.print('Применен фильтр: ' + str(list_of_filters[i]) + ' : ' + str(list_of_values[i]))
                    if download_format == 'pdf':
                        server.views.populate_pdf(view, req_option)
                    elif download_format == 'png':
                        server.views.populate_image(view, req_option)
                    if project_first:
                        filename = '%03.0f' % j + '.' + '%03.0f' % view_number + ' ' + projects_list[
                            j] + ' ' + report_name + '.' + download_format
                    else:
                        filename = '%03.0f' % view_number + '.' + '%03.0f' % j + ' ' + report_name + ' ' + \
                                   projects_list[
                                       j] + '.' + download_format
                    try:
                        with open(path + '/' + report_name + '/' + filename, 'wb') as f:
                            if download_format == 'pdf':
                                f.write(view.pdf)
                                config.print("Файл скачан")
                            elif download_format == 'png':
                                f.write(view.image)
                                config.print("Файл скачан")

                    except:
                        os.mkdir(path + '/' + report_name)
                        with open(path + '/' + report_name + '/' + filename, 'wb') as f:
                            if download_format == 'pdf':
                                f.write(view.pdf)
                                config.print("Файл скачан")
                            elif download_format == 'png':
                                f.write(view.image)
                                config.print("Файл скачан")
        else:
            for i in range(len(list_of_filters)):
                if list_of_filters[i] != '':
                    list_of_values[i] = format_date(list_of_values[i])
                    req_option.vf(list_of_filters[i], list_of_values[i])
                    config.print('Применен фильтр: ' + str(list_of_filters[i]) + ' : ' + str(list_of_values[i]))
            if download_format == 'pdf':
                config.print('Начинаю скачивать')
                server.views.populate_pdf(view, req_option)
                config.print('Файл скачан')
            elif download_format == 'png':
                server.views.populate_image(view, req_option)
            filename = '%03.0f' % view_number + ' ' + report_name + '.' + download_format
            try:
                with open(path + '/' + report_name + '/' + filename, 'wb') as f:
                    if download_format == 'pdf':
                        config.print('Записываю файл')
                        f.write(view.pdf)
                        config.print("Файл сохранен")
                    elif download_format == 'png':
                        f.write(view.image)
                        config.print("Файл скачан")
            except:
                os.mkdir(path + '/' + report_name)
                with open(path + '/' + report_name + '/' + filename, 'wb') as f:
                    if download_format == 'pdf':
                        f.write(view.pdf)
                        config.print("Файл скачан")
                    elif download_format == 'png':
                        f.write(view.image)
                        config.print("Файл скачан")


def send_plain_mail(subject, recipients, link, attachment=None, body=''):
    # Функция принимает на вход название темы, список получаетелей, путь ко вложению и текст сообщения.
    # Отправляет письмо со вложением на адреса получателей.
    msg = MIMEMultipart()
    msg['From'] = config.email
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject
    if body == '' or body == 'None':
        body = 'Автоматическая рассылка. Отчёт "' + subject + '" во вложении. Ссылка на отчёт - ' + link + ' Подготовлен Отделом методологии и ' \
                                                              'формирования отчётности.'
    msg.attach(MIMEText(body, 'plain'))
    if attachment is not None:
        with open(attachment, "rb") as f:
            attach = MIMEApplication(f.read(), _subtype="pdf")
        attach.add_header('Content-Disposition', 'attachment', filename=subject + ' ' + str(datetime.date.today()) + '.pdf')
        msg.attach(attach)
    mail_server = smtplib.SMTP(config.mail_server_name, config.mail_server_port)
    mail_server.ehlo()
    mail_server.starttls()
    mail_server.ehlo()
    mail_server.login(config.mail_server_login, config.mail_server_password)
    mail_server.send_message(msg)
    mail_server.quit()
    config.print('В ' + '%02d' % datetime.datetime.now().hour + ':' +
                 '%02d' % datetime.datetime.now().minute + ' сформировано и отправлено письмо с темой "' + subject +
                 '" на адреса: ')
    for rec in recipients:
        config.print(rec)

def send_html_mail(body, recipients, subject, attachments=[]):
    msg = MIMEMultipart()
    msg['From'] = config.email
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))
    for a in attachments:
        with open(a, "rb") as f:
            attach = MIMEApplication(f.read(), _subtype="xlsx")
        attach.add_header('Content-Disposition', 'attachment', filename=a.split('/')[-1])
        msg.attach(attach)
    mail_server = smtplib.SMTP(config.mail_server_name, config.mail_server_port)
    mail_server.ehlo()
    mail_server.starttls()
    mail_server.ehlo()
    mail_server.login(config.mail_server_login, config.mail_server_password)
    mail_server.send_message(msg)
    mail_server.quit()


def send_telegram_message(chat_id, link='', text='', file='', file_name=''):
    # Функция принимает на вход id чата в Телеграмм, текст сообщения, путь ко вложению и название отчета.
    # Отправляет сообщение и файл в указанный чат от лица бота python_reports_sender.
    if text == '' or text == 'None':
        text = 'Автоматическая рассылка отчета **' + file_name + '**. Ссылка на отчёт - ' + link + ' Подготовлен Отделом методологии и ' \
                                                                 'формирования отчётности.'

    app = Client("my_account", api_id=config.app_api_id, api_hash=config.app_api_hash, bot_token=config.bot_token)
    with app:
        app.send_message(chat_id=chat_id, text=text)
        if file:
            app.send_document(chat_id=chat_id, document=file, file_name=file_name + ' ' + str(datetime.date.today()) + '.pdf')


def get_df_from_gs(spreadsheet_id, cells_range):
    # Функция скачивания датафрейма из Google Sheet. Принимает на вход айди GS
    # и диапазон ячеек в формате строки. Возвращает датафрейм.
    credentials = ServiceAccountCredentials.from_json_keyfile_name(config.credentials_file,
                                                                   ['https://www.googleapis.com/auth/spreadsheets',
                                                                    'https://www.googleapis.com/auth/drive'])
    http_auth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build('sheets', 'v4', http=http_auth)
    request = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=cells_range,
                                                  valueRenderOption='UNFORMATTED_VALUE',
                                                  dateTimeRenderOption='FORMATTED_STRING')
    response = request.execute()
    df = pd.DataFrame(data=response['values'][1:], columns=response['values'][0])
    return df


def filter_periodicity(fields):
    # Функция, определяющая, подходит ли периодичность рассылки для сегодняшнего дня и часа.
    periodicity_type = fields[0]
    start_date = pd.to_datetime(fields[1], format='%Y-%m-%d').date()
    periodicity = int(fields[2])
    weekday_list = fields[3].split(',')
    month_list = fields[4].split(',')
    monthday_list = fields[5].split(',')
    time_list = fields[6].split(',')
    today_time = datetime.datetime.today()
    today = datetime.date.today()

    if start_date <= today and str(today_time.hour) in time_list and ((periodicity_type == 'Ежедневно' and
                                                                       (today - start_date).days % periodicity == 0) or \
                                                                      (periodicity_type == 'Еженедельно' and (
                                                                              today - start_date).days // 7 % periodicity == 0 and
                                                                       str(today.weekday()) in weekday_list) or \
                                                                      (periodicity_type == 'Ежемесячно' and
                                                                       (((today.year - start_date.year) * 12 + (
                                                                               today.month - start_date.month)) % periodicity == 0 or
                                                                        str(today.month) in month_list) and
                                                                       (
                                                                               str(today.day) in monthday_list or 'last' in monthday_list and (
                                                                               today + datetime.timedelta(
                                                                           days=1)).day == 1))):
        return True
    return False


def merge_pdfs(pdfs, path, report_name, folder=''):
    # Объединяет файлы в формате pdf в один файл, сохраняет в указанную директорию и называет указанным именем.
    # Дополнительный параметр folder принимает путь, куда нужно скопировать полученный файл.
    merger = PdfFileMerger()
    for pdf in pdfs:
        merger.append(pdf)
    merger.write(path + '/' + report_name + '/' + report_name + '.pdf')
    merger.close()
    config.print('Листы объединены в один файл')
    if folder != '':
        try:
            copyfile(path + report_name + '/' + report_name + '.pdf',
                     folder + '/' + report_name + ' ' + str(datetime.date.today()) + '.pdf')
        except:
            config.print('Не смог :(')
            pass


def clear_folder(path, report_name):
    # Очищает папку с файлами отчета
    try:
        files = os.listdir(path + '/' + report_name)
        if len(files) > 0:
            for f in files:
                os.remove(path + '/' + report_name + '/' + f)
                config.print(f + ' удален')
            config.print('Все старые файлы удалены')
    except:
        pass


def main(report_name,
         hours,
         ids,
         orientation_list,
         page_type_list,
         project_filter_name,
         projects_list,
         project_first,
         apply_project_list,
         list_of_filters,
         list_of_values,
         is_test,
         in_charge_email,
         recipients_email,
         body,
         folder,
         send_mail,
         send_telegram,
         chat_id,
         link,
         path=''):
    config.print('Начинаю работать с ' + report_name)
    if check_update(ids[0], hours):
        clear_folder(path, report_name)
        config.print('Начинаю скачивать листы')
        for i in range(len(ids)):
            download_view(ids[i],
                          i + 1,
                          report_name,
                          projects_list,
                          project_filter_name,
                          page_type_list[i],
                          orientation_list[i],
                          path,
                          str(list_of_filters[i]).split(';'),
                          str(list_of_values[i]).split(';'),
                          project_first,
                          apply_project_list
                          )
        pdfs = [path + '/' + report_name + '/' + f for f in os.listdir(path + '/' + report_name)
                if f[-3:] == 'pdf']
        merge_pdfs(pdfs, path, report_name, folder)
        if send_mail or is_test:
            recipients = in_charge_email
            if not is_test:
                recipients += recipients_email
            send_plain_mail(report_name,
                            recipients,
                            attachment=path + '/' + report_name + '/' + report_name + '.pdf',
                            body=body,
                            link=link)
        if send_telegram and not is_test:
            send_telegram_message(chat_id,
                                  text=body,
                                  file=path + '/' + report_name + '/' + report_name + '.pdf',
                                  file_name=report_name,
                                  link=link)
    else:
        send_plain_mail('Сбой рассылки отчета ' + report_name,
                        in_charge_email,
                        body='Сегодня в ' + '%02d' % datetime.datetime.now().hour + ':' + '%02d' %
                             datetime.datetime.now().minute + ' произошел сбой рассылки отчета "' + report_name +
                             '" из-за несвоевременного обновления экстракта.')

