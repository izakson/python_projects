import config
import match_data
import get_data

import pandas as pd
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib
from atlassian import Jira


attachments = [config.sales_analysis_xlsx]
attachments_logs = []


changes_reports = []





# -----------------------------------------------------------------------------------------------
# ----------------------------------------СБОР СТАТИСТИКИ----------------------------------------
# -----------------------------------------------------------------------------------------------
# Формируем итоговый источник со статистикой
def need_statistics():
    statistics_sql = "select cast(max(date) as date) as date from dbo.sales_statistics"
    statistics_sql = pd.read_sql(statistics_sql, config.bigdatatest_conn)
    need = statistics_sql['date'][0] != datetime.date.today()
    if need and not config.is_test:
        print("ВНИМАНИЕ!!! Начинаю выгрузку статистики, т.к. последняя дата ", statistics_sql['date'][0], "а сегодня",
          datetime.date.today())
    return need and not config.is_test

def collect_statistics(df_month_agg_list, list_of_metrics, statistics_data):
    for a in range(len(df_month_agg_list)):
        for period in list(df_month_agg_list[a]['rep_period']):
            for b in range(len(df_month_agg_list)-a):
                for metric in list_of_metrics:
                    if df_month_agg_list[a]['source_name'][0] == df_month_agg_list[a+b]['source_name'][0] and metric in config.list_of_metrics_clarity:
                            statistics_data['source_name'].append(df_month_agg_list[a]['source_name'][0])
                            statistics_data['rep_period'].append(period)
                            statistics_data['source_name_compare'].append(df_month_agg_list[a+b]['source_name'][0])
                            statistics_data['metrics'].append(metric)
                            statistics_data['value'].append(df_month_agg_list[a][df_month_agg_list[a]['rep_period'] == period][metric].values[0])
                            statistics_data['value_compare'].append(None)
                            statistics_data['lines'].append(df_month_agg_list[a][df_month_agg_list[a]['rep_period'] == period]['lines'].values[0])
                    elif df_month_agg_list[a]['source_name'][0] != df_month_agg_list[a+b]['source_name'][0] and metric in config.list_of_metrics_convergence:
                            statistics_data['source_name'].append(df_month_agg_list[a]['source_name'][0])
                            statistics_data['rep_period'].append(period)
                            statistics_data['source_name_compare'].append(df_month_agg_list[a+b]['source_name'][0])
                            statistics_data['metrics'].append(metric)
                            statistics_data['value'].append(df_month_agg_list[a][df_month_agg_list[a]['rep_period'] == period][metric].values[0])
                            statistics_data['value_compare'].append(df_month_agg_list[a+b][df_month_agg_list[a+b]['rep_period'] == period][metric].values[0])
                            statistics_data['lines'].append(None)

    return statistics_data

def save_statistics(statistics_data):
    statistics_df = pd.DataFrame(data=statistics_data)
    statistics_df['date'] = datetime.datetime.today()
    statistics_df.to_sql('sales_statistics', config.bigdatatest_conn, schema='dbo', if_exists='append',
                          chunksize=70, dtype=config.statistics_dtype, index=False)
    print("Статистика выгружена")


def count_changes(logs_changes_df):
    changes_dict = {}
    for metric in config.changes_intro_dict:
        if metric == 'changed_doubles':
            qty = int(logs_changes_df[logs_changes_df[metric] < logs_changes_df['doubles_old']].count()[metric])
        elif metric == 'is_gone':
            qty = sum(logs_changes_df[metric])
        else:
            qty = int(logs_changes_df.count()[metric])
        changes_dict.update({metric: qty})
    print(changes_dict)
    return changes_dict

def generate_intro(errors_dict, intro_dict, intro_intro):
    # Текст вступления сообщения, если ошибки есть
    intro_list = []
    for metric, qty in errors_dict.items():
        if qty > 0:
            intro = intro_dict[metric].lower() + ' (<span style="color: #ff0000;"><strong>' + str(qty) + \
                '</strong></span>)'
            intro_list.append(intro)
    intro = intro_intro + ", ".join(intro_list)
    return intro

def generate_changes_text(logs_changes_df, metric, intro, date='date_old'):
    changes_list = []
    text = ''
    print(metric)
    if metric == 'is_gone':
        logs_changes_df = logs_changes_df[logs_changes_df[metric]].reset_index()
    else:
        logs_changes_df = logs_changes_df[pd.notnull(logs_changes_df[metric])].reset_index()
    print(logs_changes_df)
    if len(logs_changes_df) > 0:
        for i in range(len(logs_changes_df)):
            print(logs_changes_df[date][i])
            print(logs_changes_df[date][i].month)
            print(config.month_dict[logs_changes_df[date][i].month])
            text = '<li>' + logs_changes_df['operation'][i] + ' <strong>' + \
                              logs_changes_df['cid'][i] + '</strong> (' + logs_changes_df['oid'][
                                  i] + '). Отчетный период: <strong>' \
                              + config.month_dict[logs_changes_df[date][i].month] + ' ' + str(
                logs_changes_df[date][i].year) + '</strong>.'
            if metric == 'is_gone' or metric =='is_new_date':
                end_text = '</li>'
            elif metric == 'changed_doubles':
                end_text = ' Было записей: <span style="color: #ff0000;"><strong>' \
                               + str(logs_changes_df['doubles_old'][
                                         i] + 1) + '</strong></span>, стало записей: <span style="color: #ff0000;"><strong>' \
                               + str(logs_changes_df['changed_doubles'][i] + 1) + '</strong></span>.</li>'
            else:
                end_text = ' Старое значение: <span style="color: #ff0000;"><strong>' \
                                             + str(logs_changes_df[metric.split('_')[1]+'_old'][
                                                       i]) + '</strong></span>, новое значение: <span style="color: #ff0000;"><strong>' \
                                             + str(
                    logs_changes_df[metric][i]) + '</strong></span>.</li>'
            changes_list.append(text+end_text)
        text = '<li>' + intro + " ".join(changes_list) + '</ul></li>'
    print(text)
    return text


def generate_changes_report_text(logs_changes_df, merged_logs, source_name):
    intro_intro = 'За временной отрезок с ' + str(merged_logs['datetime_old'][0]) + ' по ' \
                  + str(merged_logs['datetime_new'][0]) + ' зафиксированы '
    intro = generate_intro(count_changes(logs_changes_df), config.changes_intro_dict, intro_intro)
    gone_lines_text = generate_changes_text(logs_changes_df, 'is_gone', '<li>Пропали записи: <ul>')
    new_lines_text = generate_changes_text(logs_changes_df, 'is_new_date', '<li>Появились записи в закрытых периодах: <ul>', 'is_new_date')
    new_doubles_text = generate_changes_text(logs_changes_df, 'changed_doubles', '<li>Появились дубли: <ul>')
    changed_dates_text = generate_changes_text(logs_changes_df, 'changed_date', '<li>Изменился отчетный период: <ul>')
    changed_qtys_text = generate_changes_text(logs_changes_df, 'changed_qty', '<li>Изменилось количество: <ul>')
    changed_sqrs_text = generate_changes_text(logs_changes_df, 'changed_sqr', '<li>Изменился объем: <ul>')
    changed_rubs_text = generate_changes_text(logs_changes_df, 'changed_rub', '<li>Изменилась сумма выручки: <ul>')
    changed_compensations_text = generate_changes_text(logs_changes_df, 'changed_compensation', '<li>Изменилась сумма компенсации: <ul>')
    changed_cost_num_text = generate_changes_text(logs_changes_df, 'changed_cost_num',
                                                       '<li>Изменился числитель цены: <ul>')
    changed_cost_denom_text = generate_changes_text(logs_changes_df, 'changed_cost_denom',
                                                       '<li>Изменился знаменатель цены: <ul>')
    changes_report = '<h3>' + source_name + '</h3>' + \
                     '<p>' + intro + '.</p><ul>' + \
                     gone_lines_text + new_lines_text + new_doubles_text + \
                     changed_dates_text + changed_qtys_text + changed_sqrs_text + changed_rubs_text + \
                     changed_compensations_text + changed_cost_num_text + changed_cost_denom_text + '</ul>'
    changes_reports.append(changes_report)


def count_errors(sales_analysis):
    errors_dict = {}
    for metric in list(config.convergence_errors_intro_dict.keys()):
        errors_dict.update({metric: int(sum(sales_analysis[metric]))})
    for metric in config.clarity_errors_intro_dict:
        qty = 0
        for suffix in get_data.suffixes.keys():
            qty += int(sum(sales_analysis[metric + suffix].fillna(0)))
        errors_dict.update({metric: qty})
    return errors_dict


def find_new(df_old, df_new):
    df_merged = df_old.merge(df_new, how='outer', on=config.ids, suffixes=('_old', '_new'))
    df_merged['is_new'] = pd.isnull(df_merged['date_old'])
    df_new['is_new'] = df_new.merge(df_merged, how='left', on=config.ids)['is_new']
    return df_new


def form_errors_df(sales_analysis, metric, path, tf=False):
    if tf:
        df = sales_analysis[sales_analysis[metric]]
    else:
        sales_analysis[metric] = sales_analysis[metric].fillna(0)
        df = sales_analysis[sales_analysis[metric]>0]
    df = df.reset_index()
    df.columns = list(df.columns)
    if len(df):
        try:
            df = find_new(pd.read_excel(path), df)
        except:
            df['is_new'] = True
        df.to_excel(path)
        attachments.append(path)
    return df

def generate_is_absent_text(df):
    list = []
    header = '<h3><span style="text-decoration: underline;">' \
                    'Требуется проверить корректность наличия/отсутствия данных записей:</span></h3>'
    # Проходим по каждой строке из датафрейма и собираем части сообщения
    for i in range(len(df)):
        reports_in = []  # Список для частей сообщения об источниках, где запись есть
        reports_out = []  # Список для частей сообщения об источниках, где записи нет
        # Проходим по каждому источнику и собираем части сообщения
        for suffix in get_data.suffixes.keys():
            if not df['date' + suffix][i] is pd.NaT and df['date' + suffix][i]:
                report_in = '<span style="color: #385c1f;"><strong>' + \
                            get_data.suffixes[suffix] + '</strong></span> (отчетный период: <strong>' + \
                            config.month_dict[df['date' + suffix][i].month] + ' ' + \
                            str(df['date' + suffix][i].year) + '</strong>)'
                reports_in.append(report_in)
            else:
                report_out = '<span style="color: #ff0000;"><strong>' + get_data.suffixes[suffix] + '</strong></span>'
                reports_out.append(report_out)

        is_new_text = ''
        if df['is_new'][i]:
            is_new_text = '<span style="color: #ff0000;"><strong><span style="background-color: #ffff00;">' \
                          'NEW!</span></strong></span> '
        absent_text = '<li>' + is_new_text + df['operation'][i] + ' <strong>' + \
                      df['cid'][i] + \
                      '</strong> (' + df['oid'][
                          i] + '). Запись присутствует в следующих источниках: ' + \
                      ", ".join(reports_in) + '. Запись отсутствует в следующих источниках: ' + \
                      ", ".join(reports_out) + '.</li>'
        if df['is_new'][i]:
            config.list_of_new_absent.append(absent_text)
        list.append(absent_text)
    if len(df):
        text = header + '<ol>' + " ".join(list) + '</ol>'
    else:
        text = ''
    return text

def generate_descr_text(df, metric, list_of_new_descr):
    list_of_descr = []
    header_descr = '<h3><u>' + config.convergence_errors_intro_dict[metric] + '</u></h3>'
    for i in range(len(df)):
        reports_in = []
        for suffix in get_data.suffixes.keys():
            if not df['date' + suffix][i] is pd.NaT:
                    report_in = '<li>' + get_data.suffixes[suffix] + '''. Отчетный период: <strong>
                                             ''' + config.month_dict[
                        df['date' + suffix][i].month] + ' ' + str(
                        df['date' + suffix][i].year) + '''</strong>. Значение: <span style="color: #ff0000;"><strong>''' + \
                                str(df[metric[6:] + suffix][i]) + '</strong></span> ₽.</li>'
                    reports_in.append(report_in)
        is_new_text = ''
        if df['is_new'][i]:
            is_new_text = '<span style="color: #ff0000;"><strong><span style="background-color: #ffff00;">' \
                              'NEW!</span></strong></span> '
        descr_text = '<li>' + is_new_text + df['operation'][i] + ' <strong>' + df['cid'][i] + '</strong>(' + df['oid'][i] + '). <ul>' + \
                                      " ".join(reports_in) + '</ul></li>'
        if df['is_new'][i]:
            list_of_new_descr.append(descr_text)
        list_of_descr.append(descr_text)
    if len(df):
        text = header_descr + '<ol>' + " ".join(list_of_descr) + '</ol>'
    else:
        text = ''
    return text


def generate_wrongs_text(df, metric, suffix, list_of_new_wrongs):
    list_of_wrongs = []
    for i in range(len(df)):
        error_type = df[metric+'_type'+suffix][i]
        is_new_text = ''
        if df['is_new'][i]:
            is_new_text = '<span style="color: #ff0000;"><strong><span style="background-color: #ffff00;">' \
                              'NEW!</span></strong></span> '
        wrongs_text = '''<li>''' + is_new_text + df['operation'][i] + ''' <strong>''' + \
                          df['cid'][i] + '''</strong>(''' + df['oid'][i] + '''). Отчетный период: <strong>
                                                         ''' + config.month_dict[df['date' + suffix][i].month] + ' ' + str(
            df['date' + suffix][i].year) + '''</strong>. <span style="color: #ff0000;">''' + \
                          str(error_type) + '</span>.</li>'
        if df['is_new'][i]:
            list_of_new_wrongs.append(wrongs_text)
        list_of_wrongs.append(wrongs_text)
    if len(df):
        text = '<li>'+config.clarity_errors_intro_dict[metric]+': <ul>' + " ".join(list_of_wrongs) + '</ul></li>'
    else:
        text = ''
    return text

def generate_doubles_text(df, metric, suffix, list_of_new_wrongs):
    list_of_doubles = []
    for i in range(len(df)):
        is_new_text = ''
        if df['is_new'][i]:
            is_new_text = '<span style="color: #ff0000;"><strong><span style="background-color: #ffff00;">' \
                              'NEW!</span></strong></span> '
        doubles_text = '''<li>''' + is_new_text + df['operation'][i] + ''' <strong>''' + \
                       df['cid'][i] + '''</strong>(''' + df['oid'][i] + '''). Отчетный период: <strong>
                                                         ''' + config.month_dict[
                           df['date' + suffix][i].month] + ' ' + str(
            df['date' + suffix][i].year) + '''</strong>. Количество одинаковых записей: 
                                            <strong><span style="color: #ff0000;">''' + str(
            int(df['doubles' + suffix][i] + 1)) + \
                       '</span></strong>.</li>'
        if df['is_new'][i]:
            list_of_new_wrongs.append(doubles_text)
        list_of_doubles.append(doubles_text)
    if len(df):
        text = '<li><span style="text-decoration: underline;">Дубли:</span><ul>' + " ".join(
                    list_of_doubles) + '</ul></li>'
    else:
        text = ''
    return text



def generate_reports_text(sales_analysis, path):
    wrongs_reports = []
    header_reports = '<h3><u>Аномальные знаения в источниках:</u></h3>'
    # Проходим по каждому отчету и собираем части сообщения
    for suffix in get_data.suffixes.keys():
        wrongs = []
        doubles = []
        metric = 'wrong_qty'
        wrongs_df = form_errors_df(sales_analysis, metric + suffix, path + get_data.suffixes[suffix] + '. ' + config.clarity_errors_intro_dict[metric]+'.xlsx')
        wrongs.append(generate_wrongs_text(wrongs_df, metric, suffix, config.list_of_new_wrongs_qty))
        metric = 'wrong_sqr'
        wrongs_df = form_errors_df(sales_analysis, metric + suffix,
                                   path + get_data.suffixes[suffix] + '. ' + config.clarity_errors_intro_dict[metric]+'.xlsx')
        wrongs.append(generate_wrongs_text(wrongs_df, metric, suffix, config.list_of_new_wrongs_sqr))
        metric = 'wrong_rub'
        wrongs_df = form_errors_df(sales_analysis, metric + suffix,
                                   path + get_data.suffixes[suffix] + '. ' + config.clarity_errors_intro_dict[metric]+'.xlsx')
        wrongs.append(generate_wrongs_text(wrongs_df, metric, suffix, config.list_of_new_wrongs_rub))
        metric = 'wrong_compensation'
        wrongs_df = form_errors_df(sales_analysis, metric + suffix,
                                   path + get_data.suffixes[suffix] + '. ' + config.clarity_errors_intro_dict[metric]+'.xlsx')
        wrongs.append(generate_wrongs_text(wrongs_df, metric, suffix, config.list_of_new_wrongs_compensation))
        metric = 'wrong_cost_num'
        wrongs_df = form_errors_df(sales_analysis, metric + suffix,
                                   path + get_data.suffixes[suffix] + '. ' + config.clarity_errors_intro_dict[
                                       metric] + '.xlsx')
        wrongs.append(generate_wrongs_text(wrongs_df, metric, suffix, config.list_of_new_wrongs_cost_num))
        metric = 'wrong_cost_denom'
        wrongs_df = form_errors_df(sales_analysis, metric + suffix,
                                   path + get_data.suffixes[suffix] + '. ' + config.clarity_errors_intro_dict[
                                       metric] + '.xlsx')
        wrongs.append(generate_wrongs_text(wrongs_df, metric, suffix, config.list_of_new_wrongs_cost_denom))

        metric = 'doubles'
        doubles_df = form_errors_df(sales_analysis, metric + suffix,
                                   path + get_data.suffixes[suffix] + '. ' + config.clarity_errors_intro_dict[metric]+'.xlsx')
        doubles_text = generate_doubles_text(doubles_df, metric, suffix, config.list_of_new_doubles)

        if len(wrongs) > 0:
            wrongs_text = '<li><span style="text-decoration: underline;">Аномальные значения:</span><ul>' + " ".join(
                    wrongs) + '</ul></li>'
        else:
            wrongs_text = ''

        if len(doubles) + len(wrongs) > 0:
            wrongs_report = '<li><h4><strong>' + get_data.suffixes[suffix] + '</strong></h4><ul>' + doubles_text + wrongs_text + '</ul>'
            wrongs_reports.append(wrongs_report)
    if len(wrongs_reports):
        text = header_reports + '<ol>' + " ".join(wrongs_reports) + '</ol>'
    else:
        text = ''
    return text

def generate_errors_text(sales_analysis):
    intro = generate_intro(count_errors(sales_analysis),
                           {**config.convergence_errors_intro_dict, **config.clarity_errors_intro_dict},
                           'По результатам проверки найдены: ')
    is_absent_text = generate_is_absent_text(form_errors_df(sales_analysis,'is_absent',config.is_absent_xlsx,True))
    descr_qty_text = generate_descr_text(form_errors_df(sales_analysis,'descr_qty',config.descr_qty_xlsx,True), 'descr_qty', config.list_of_new_descr_qty)
    descr_sqr_text = generate_descr_text(form_errors_df(sales_analysis, 'descr_sqr', config.descr_sqr_xlsx,True), 'descr_sqr',
                                         config.list_of_new_descr_sqr)
    descr_rub_text = generate_descr_text(form_errors_df(sales_analysis, 'descr_rub', config.descr_rub_xlsx,True), 'descr_rub',
                                         config.list_of_new_descr_rub)
    descr_compensation_text = generate_descr_text(form_errors_df(sales_analysis, 'descr_compensation', config.descr_compensation_xlsx,True), 'descr_compensation',
                                         config.list_of_new_descr_compensation)
    descr_cost_num_text = generate_descr_text(form_errors_df(sales_analysis, 'descr_cost_num', config.descr_cost_num_xlsx,True), 'descr_cost_num',
                                         config.list_of_new_descr_cost_num)
    descr_cost_denom_text = generate_descr_text(form_errors_df(sales_analysis, 'descr_cost_denom', config.descr_cost_denom_xlsx,True), 'descr_cost_denom',
                                         config.list_of_new_descr_cost_denom)
    wrong_reports_text = generate_reports_text(sales_analysis, config.path)

    body = '''\
        <html>
          <head></head>
          <body>
            <p>Это автоматическая рассылка результатов анализа данных по продажам. 
            Выгрузка с построчным сравнением во вложении.</p>
            <p>&nbsp;</p>
            <p>''' + intro + '''.</p>
             <p>&nbsp;</p>''' + is_absent_text + \
           descr_qty_text + descr_sqr_text + descr_rub_text + \
           descr_compensation_text + descr_cost_num_text + descr_cost_denom_text + wrong_reports_text + \
           '''</body>
        </html>
        '''
    return body

def send_html_mail(body, recipients, subject, attachments):
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


def create_jira_task(issuetype, project_key, labels, assignee_name, summary, description):
    if description != ' ':
        jira = config.jira
        jira.issue_create(fields={
            'project': {'key': project_key},
            'issuetype': {"name": issuetype},
            'summary': summary,
            'labels': labels,
            'assignee': {'name': assignee_name},
            'description': description
        })


def main():
    if need_statistics():
        statistics_data = collect_statistics(get_data.df_month_agg_list, config.list_of_metrics, config.statistics_data)
        save_statistics(statistics_data)
    for i in range(len(match_data.logs_changes_df_list)):
        logs_changes_df = match_data.logs_changes_df_list[i]
        merged_logs = match_data.merged_logs_list[i]
        source_name = list(get_data.suffixes.values())[i]
        if len(logs_changes_df) > 0:
            print(logs_changes_df.info())
            logs_changes_df = logs_changes_df.reset_index()
            generate_changes_report_text(logs_changes_df, merged_logs, source_name)
    recipients = config.in_charge
    if not config.is_test:
        recipients += config.recipients
        print('is not test')
    if len(changes_reports):

        changes_body = '''<html><head></head>
              <body>
                <p>Это автоматическая рассылка результатов анализа изменений данных по продажам. 
                Выгрузка с построчным сравнением во вложении.</p>''' + \
           " ".join(changes_reports) + \
           '''</body>
        </html>
        '''
        changes_subject = "Результаты анализа изменений данных по продажам"
        send_html_mail(changes_body, recipients, changes_subject, attachments_logs)
    if sum(count_errors(match_data.sales_analysis).values()):
        errors_body = generate_errors_text(match_data.sales_analysis)
    else:
        errors_body = '''\
        <html>
          <head></head>
          <body>
            <p>Это автоматическая рассылка результатов анализа данных по продажам. 
            Выгрузка с построчным сравнением во вложении.</p>
            <p>&nbsp;</p>
            <p>По результатам проверки ошибок не найдено.</p></body>
        </html>
        '''
    errors_subject = "Результаты анализа данных по продажам"
    send_html_mail(errors_body, recipients, errors_subject, attachments)
    # if not config.is_test:
    #     metrics_dict = {**config.convergence_errors_intro_dict, **config.clarity_errors_intro_dict}
    #     for metric in list(config.jira_texts_dict.keys()):
    #         create_jira_task('Проблема',
    #                              config.jira_project_key,
    #                              config.jira_labels,
    #                              's.izakson',
    #                              metrics_dict[metric],
    #                              '{html}<ol>' + config.jira_texts_dict[metric] + '</ol>{html}')
    #
