# Файл с основными функциями для скачивания, форматирования и рассылки сложных отчетов (текстовые, отчеты с генерацией листов)

from fpdf import FPDF
import datetime
import time
import os
import sqlalchemy
import pyodbc
import pandas as pd

import config
import simple_sender


def create_intro_slide(image, font, month, report_name, date=datetime.date.today(), font_path='', path=''):
    # Функция устанавливает формат, шрифт, цвет текста pdf файла, пишет заголовки (создает обложку отчета)
    pdf = FPDF(orientation='L', unit='mm', format='legal')
    pdf.add_page()
    pdf.image(image, x=0, y=0, w=357, h=217, type='', link='')
    try:
        pdf.set_font(font, '', 34)
    except:
        pdf.add_font(font, '', font_path, uni=True)
        pdf.set_font(font, '', 34)
    pdf.set_text_color(255, 255, 255)
    # pdf.cell(ln=200, h=40, align='L', w=0, txt="продажи за", border=0,fill = False)
    pdf.text(17, 120, "продажи " + config.month_dict[month])
    pdf.text(17, 140, str(date))
    try:
        pdf.output(path + '/' + report_name + '/' + '000.000.intro.pdf')
    except:
        os.mkdir(path + '/' + report_name)
        pdf.output(path + '/' + report_name + '/' + '000.000.intro.pdf')
    config.print("Создан титульный слайд")


def merge_images_pdf(images, report_name, header, number_of_slides, number_of_projects, slide_w, slide_h, columns, rows,
                     logo, font,
                     month, year, slide_number, path='', font_path=''):
    # Функция принимает на вход список слайдов в формате png, которые мы хотим разместить на одном листе, название отчета,
    # заголовок слайда, количество слайдов, число проектов, ширину и высоту слайдов, количество колонок и строк, на которые будет 
    # разбит лист, логотип компании, шрифт текста, месяц и год отчета, порядковый номер слайда. В итоге сохраняет все получившиеся листы в формате pdf.
    pdf = FPDF(orientation='L', unit='mm', format='legal')
    w = slide_w / columns
    h = slide_h / rows
    for slide in range(number_of_slides):
        list_of_slides = [f for f in images if int(f[:3]) == slide + 1]
        i = 0
        while i < number_of_projects:
            pdf.add_page()
            for k in range(rows):
                for j in range(columns):
                    try:
                        pdf.image(path + '/' + report_name + '/' + header + '/' + list_of_slides[i + j + k * columns],
                                  x=w * j, y=h * k, w=w, h=h, type='', link='')
                    except:
                        pass
            pdf.image(logo, x=305, y=5, w=47, h=6)
            try:
                pdf.set_font(font, '', 14)
            except:
                pdf.add_font(font, '', font_path, uni=True)
                pdf.set_font(font, '', 14)
            pdf.set_text_color(3, 100, 180)
            pdf.text(4, 8, header)
            pdf.text(slide_w * 0.75, 8, config.month_full_dict[int(month)] + ' ' + str(year))
            config.print('Готова страница ' + str(i // (rows * columns) + 1))
            i += rows * columns
    pdf.output(path + '/' + report_name + '/' + '%03.0f' % slide_number + ' ' + header + '.pdf')


def get_sql_data(db_name,
                 list_of_filters,
                 list_of_values, parameters=False, table_name='', sql=''):
    # Функция принимает на вход название базы данных, список фильтров и их значений, название таблицы и запрос на sql. Возвращает
    # датафрейм с полученными данными для текстового отчета.
    config.print("Подключаюсь к БД")
    filters = []
    parameters_list = []
    parameters_string = ''
    config.print(str(db_name))
    if sql == '':
        config.print(str(table_name))
    db_params = config.db_dict[str(db_name)]

    engine = sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect={}".format(db_params), fast_executemany=True)
    conn = engine.raw_connection()
    config.print("Успешно подключился к БД")
    if parameters:
        for i in range(len(str(list_of_filters).split(';'))):
            filter = str(list_of_filters).split(';')[i]
            value = str(list_of_values).split(';')[i]
            parameters_list.append('@' + str(filter) + '=' + str(value))
        parameters_string = ','.join(parameters_list)
        config.print(parameters_string)
    if sql != '':

        for i in range(len(str(list_of_filters).split(';'))):
            filter = str(list_of_filters).split(';')[i]
            value = str(list_of_values).split(';')[i]
            filters.append(str(filter) + "='" + str(value) + "'")
        filters_string = ' and '.join(filters)
        config.print(filters_string)
    if sql == '':
        #sql = "GO \n DECLARE @return_value int EXEC @return_value = " + str(table_name) + " " + parameters_string + " SELECT 'Return Value' = @return_value \n  GO \n "
        sql = "EXEC " + str(table_name) + " " + parameters_string
    elif len(filters):
        sql += ' where ' + filters_string
    report_df = pd.read_sql(sql, conn)
    return report_df

def weird_division(n, d):
    return n / d if d else 0

def summary_text(df, header):
    ## Функция принимает на вход датафрейм с данными и заголовок для сбора текстового отчета "Summary", возвращает полученный текст.
    text = "#summary **" + header + ' %02d' % datetime.date.today().day + '.' + '%02d' % datetime.date.today().month + '.' \
           + '%d' % datetime.date.today().year + '**\n' + \
           '\n' + \
           "**За день:**\n" + \
           "Встречи всего: " + '%d' % sum(df['День_ВстречиВсего']) + '\n' + \
           "Первичные встречи: " + '%d' % sum(df['День_ПервичныеВстречиВсего']) + '\n' + \
           "\t в офисе - " + '%d' % sum(df['День_ПервичныеВстречиНаОбъекте']) + '\n' + \
           "\t онлайн - " + '%d' % sum(df['День_ПервичныеВстречиОнлайн']) + '\n' + \
           "Резервы: " + '%d шт' % sum(df['День_Резервы_КолВо']) + ' / ' + '%d м²' % sum(df['День_Резервы_Площадь']) +\
            ' / ' + '%0.3f млн руб' % (sum(df['День_Резервы_Сумма'])/1000000) + ' / ' + '{0:,} руб/м²'.format(round(weird_division(sum(df['День_Резервы_Сумма']), sum(df['День_Резервы_Площадь'])))).replace(',', ' ') + '\n' + \
           "Опционы: " + '%d шт' % sum(df['День_Опционы_КолВо']) + ' / ' + '%d м²' % sum(df['День_Опционы_Площадь']) +\
            ' / ' + '%0.3f млн руб' % (sum(df['День_Опционы_Сумма'])/1000000) + ' / ' + '{0:,} руб/м²'.format(round(weird_division(sum(df['День_Опционы_Сумма']), sum(df['День_Опционы_Площадь'])))).replace(',', ' ') + '\n' + \
            "ДДУ в подготовке: " + '%d шт' % sum(df['День_ДДУвПодготовкеКолВо']) + ' / ' + '%d м²' % sum(df['День_ДДУвПодготовкеПлощадь']) +\
            ' / ' + '%0.3f млн руб' % (sum(df['День_ДДУвПодготовкеСумма'])/1000000) + ' / ' + '{0:,} руб/м²'.format(round(weird_division(sum(df['День_ДДУвПодготовкеСумма']), sum(df['День_ДДУвПодготовкеПлощадь'])))).replace(',', ' ') + '\n' + \
            "Сделки: " + '%d шт' % (sum(df['День_Продажа_КолВо'])) + ' / ' + '%d м²' % (sum(df['День_Продажа_Площадь'])) +\
            ' / ' + '%0.3f млн руб' % ((sum(df['День_Продажа_Сумма']))/1000000) + ' / ' + '{0:,} руб/м²'.format(round(weird_division(sum(df['День_Продажа_1квм_числитель']), sum(df['День_Продажа_1квм_знаменатель'])))).replace(',', ' ') + '\n' + \
            "Расторжения: " + '%d шт' % sum(df['День_Расторжение_КолВо']) + ' / ' + '%d м²' % sum(df['День_Расторжение_Площадь']) +\
            ' / ' + '%0.3f млн руб' % (sum(df['День_Расторжение_Сумма'])/1000000) + ' / ' + '{0:,} руб/м²'.format(round(weird_division(sum(df['День_Расторжение_1квм_числитель']), sum(df['День_Расторжение_1квм_знаменатель'])))).replace(',', ' ') + '\n' + \
           "Одобрено: " + '%d' % sum(df['День_Одобрено']) + '\n' + \
           '\n' + \
           "**Конверсия:**\n" + \
           "Резерв / встреча: " + "{:.0%}".format(weird_division(sum(df['День_РезервыСПервичныхВстреч']), sum(df['День_ПервичныеВстречиВсего']))) + '\n' + \
           "Опцион / встреча: " + "{:.0%}".format(weird_division(sum(df['День_ОпционыСПервичныхВстреч']), sum(df['День_ПервичныеВстречиВсего']))) + '\n' + \
           "Подача / встреча: " + "{:.0%}".format(weird_division(sum(df['День_Подано']), sum(df['День_ПервичныеВстречиВсего']))) + '\n' + \
           '\n' + \
           "**За месяц:**\n" + \
           "Резервы: " + '%d шт' % sum(df['Период_Резервы_КолВо']) + ' / ' + '%d м²' % sum(df['Период_Резервы_Площадь']) + \
           ' / ' + '%0.3f млн руб' % (sum(df['Период_Резервы_Сумма']) / 1000000) + ' / ' + '{0:,} руб/м²'.format(round(weird_division(
            sum(df['Период_Резервы_Сумма']), sum(df['Период_Резервы_Площадь'])))).replace(',', ' ') + '\n' + \
           "Опционы: " + '%d шт' % sum(df['Период_Опционы_КолВо']) + ' / ' + '%d м²' % sum(df['Период_Опционы_Площадь']) + \
           ' / ' + '%0.3f млн руб' % (sum(df['Период_Опционы_Сумма']) / 1000000) + ' / ' + '{0:,} руб/м²'.format(round(weird_division(
            sum(df['Период_Опционы_Сумма']), sum(df['Период_Опционы_Площадь'])))).replace(',', ' ') + '\n' + \
           "ДДУ в подготовке: " + '%d шт' % sum(df['Период_ДДУвПодготовкеКолВо']) + ' / ' + '%d м²' % sum(
            df['Период_ДДУвПодготовкеПлощадь']) + \
           ' / ' + '%0.3f млн руб' % (
                       sum(df['Период_ДДУвПодготовкеСумма']) / 1000000) + ' / ' + '{0:,} руб/м²'.format(round(weird_division(
            sum(df['Период_ДДУвПодготовкеСумма']), sum(df['Период_ДДУвПодготовкеПлощадь'])))).replace(',', ' ') + '\n' + \
           "Сделки: " + '%d шт' % (sum(df['Период_Продажа_КолВо'])) + ' / ' + '%d м²' % (sum(df['Период_Продажа_Площадь'])) + \
           ' / ' + '%0.3f млн руб' % ((sum(df['Период_Продажа_Сумма'])) / 1000000) + ' / ' + '{0:,} руб/м²'.format(round(weird_division(
            sum(df['Период_Продажа_1квм_числитель']), sum(df['Период_Продажа_1квм_знаменатель'])))).replace(',', ' ') + ' ' + "({:.1%})".format(weird_division((sum(df['Период_Продажа_Сумма'])), sum(df['ПланПродажСтоимость']))) + '\n' + \
           "Расторжения: " + '%d шт' % sum(df['Период_Расторжение_КолВо']) + ' / ' + '%d м²' % sum(
            df['Период_Расторжение_Площадь']) + \
           ' / ' + '%0.3f млн руб' % (
                       sum(df['Период_Расторжение_Сумма']) / 1000000) + ' / ' + '{0:,} руб/м²'.format(round(weird_division(sum(df['Период_Расторжение_1квм_числитель']),sum(df['Период_Расторжение_1квм_знаменатель'])))).replace(',', ' ') + '\n' + \
           "Одобрено: " + '%d' % sum(df['Период_Одобрено']) + '\n' + \
           '\n' + \
           "Сделки по форме оплаты: \n" + \
           "100% оплата - " + "{:.0%}".format(weird_division(sum(df['Период_100%_оплата']), sum(df['Период_Продажа_КолВо']))) + '\n' + \
           "Ипотека - " + "{:.0%}".format(weird_division(sum(df['Период_Ипотека_оплата']+df['Период_РВИ_оплата']), sum(df['Период_Продажа_КолВо']))) + '\n' + \
           "Рассрочка - " + "{:.0%}".format(weird_division(sum(df['Период_Рассрочка_оплата']), sum(df['Период_Продажа_КолВо']))) + '\n' + \
           "Субсидия - " + "{:.0%}".format(weird_division(sum(df['Период_Субсидия_оплата']), sum(df['Период_Продажа_КолВо']))) + '\n' + \
           '\n' + \
           "**Конверсия:**\n" + \
           "Опцион / встреча: " + "{:.0%}".format(weird_division(sum(df['А2_Период_Опционы_КолВо']), sum(df['Период_ПервичныеВстречиВсего']))) + '\n' + \
           "Сделка / встреча: " + "{:.0%}".format(weird_division(sum(df['Период_Продажа_КолВо']), sum(df['Период_ПервичныеВстречиВсего']))) + '\n' + \
           "Сделка / бронь: " + "{:.0%}".format(weird_division(sum(df['Период_Продажа_КолВо']), sum(df['А2_Период_Опционы_КолВо']))) + '\n' + \
           "Доля сделок с опцией Кухня: " + "{:.0%}".format(weird_division(sum(df['Период_Продажа_СКухней_КолВо']), sum(df['Период_Продажа_СКухнейОтказ_КолВо']+df['Период_Продажа_СКухней_КолВо']))) + '\n' + \
           "Подача / встреча: " + "{:.0%}".format(weird_division(sum(df['Период_Подано']), sum(df['Период_ПервичныеВстречиВсего']))) + '\n' + \
           "Подано / одобрено: " + "{:.0%}".format(weird_division(sum(df['Период_Одобрено']), sum(df['Период_Подано']))) + '\n' + \
           "Одобрено / подписано: " "{:.0%}".format(weird_division(sum(df['Период_СделкиИпотека_КолВо']), sum(df['Период_Одобрено']))) + '\n' + \
           '\n' + \
           "Просрочки по передаче: " + '%d' % sum(df['ПросрочкиПоПередачеКолВо'])
    return text



def metrics_analytics_sender(report_name,
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
                             month,
                             year,
                             send_mail,
                             send_telegram,
                             chat_id,
                             link,
                             path=''):
    ## Функция для формирования и отправки отчета с генерацией листов. Объединяет выше написанные функции: очистка папки от старых
    # файлов, создает обложку, скачивает листы отчета и картинки отчета, которые мы хотим нарезать и продублировать в отчете,
    # объединяет их и размещает на pdf листах отчета, отправляет готовый отчет по почте и/или в Телеграм.
    config.print('Начинаю работать с ' + report_name)
    if simple_sender.check_update(ids[0], hours):
        simple_sender.clear_folder(path, report_name)
        try:
            os.remove(path + '/' + report_name + '/' + report_name + '.pdf')
        except:
            pass
        create_intro_slide(config.ma_intro_slide_image,
                           config.cofo_font,
                           month,
                           report_name,
                           font_path=config.cofo_font_path,
                           path=path)
        config.print('Начинаю скачивать листы')
        for i in range(len(ids)):
            simple_sender.download_view(ids[i],
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
        config.print('Начинаю скачивать картинки Summary')

        projects_list.pop(0)
        number_of_projects = len(projects_list)
        ids_summary = config.ma_summary_list
        number_of_summary = len(ids_summary)
        simple_sender.clear_folder(path + '/' + report_name, config.ma_summary_header)
        for i in range(number_of_summary):
            simple_sender.download_view(ids_summary[i],
                                        i + 1,
                                        config.ma_summary_header,
                                        projects_list,
                                        project_filter_name,
                                        list_of_filters=str(list_of_filters[i]).split(';'),
                                        list_of_values=str(list_of_values[i]).split(';'),
                                        project_first=False,
                                        apply_project_list=[str(i + 1)],
                                        download_format='png',
                                        path=path + '/' + report_name
                                        )
        images_summary = [x for x in os.listdir(path + '/' + report_name + '/' + config.ma_summary_header) if
                          x[-3:] == 'png']
        merge_images_pdf(images_summary,
                         report_name,
                         config.ma_summary_header,
                         number_of_summary,
                         number_of_projects,
                         config.legal_width,
                         config.legal_hight,
                         4,
                         1,
                         config.logo,
                         config.cofo_font,
                         month,
                         year,
                         slide_number=number_of_projects + 1,
                         path=path,
                         font_path=config.cofo_font_path)

        pdfs = [path + '/' + report_name + '/' + f for f in os.listdir(path + '/' + report_name)
                if f[-3:] == 'pdf']
        simple_sender.merge_pdfs(pdfs, path, report_name, folder)
        recipients = in_charge_email
        if not is_test:
            recipients += recipients_email
        if send_mail:
            recipients = in_charge_email
            if not is_test:
                recipients += recipients_email
            simple_sender.send_plain_mail(report_name,
                                          recipients,
                                          attachment=path + '/' + report_name + '/' + report_name + '.pdf',
                                          body=body,
                                          link=link)
        if send_telegram:
            simple_sender.send_telegram_message(chat_id,
                                                text=body,
                                                file=path + '/' + report_name + '/' + report_name + '.pdf',
                                                file_name=report_name)
    else:
        simple_sender.send_plain_mail('Сбой рассылки отчета ' + report_name,
                                      in_charge_email,
                                      body='Сегодня в ' + '%02d' % datetime.datetime.now().hour + ':' + '%02d' %
                                           datetime.datetime.now().minute + ' произошел сбой рассылки отчета "'
                                           + report_name + '" из-за несвоевременного обновления экстракта.')
