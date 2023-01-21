# Итоговый скрипт с порядком выполнения предыдущих скриптов.

import simple_sender
import special_sender
import config

# Обновляем файл со списком всех дашбордов
path = config.path
list_of_df = []

try:
    simple_sender.update_views(path)
    config.print("Файл со списком дашбордов обновлен")
except:
    config.print("Файл со списком дашбордов НЕ обновлен")
    pass

# Получаем данные из GS с пользовательскими настройками рассылок отчетов
df = simple_sender.get_df_from_gs(config.spreadsheet_id, "simple_sender!A1:ZZ")
config.print("Получен GS")
# Выбираем только те отчеты, которые нужно отправлять сегодня в этот час согласно периодичности
df['to_send'] = df[config.list_of_periodicity_fields].apply(
    simple_sender.filter_periodicity,
    axis=1)
if config.only_force_start:
    df = df.query("force_start")
else:
    df = df.query("(to_send or force_start) and is_on")

config.print('Количество отчетов для рассылки: '+str(len(df)))
## Обрабатываем информацию о требуемых отчетах
for index, row in df.iterrows():
    #try:
        send_mail = row['send_mail']
        send_telegram = row['send_telegram']
        chat_id = config.chat_id_dict[str(row['tg_channel'])]

        report_name = str(row['report_name'])
        hours = int(row['hours'])
        ids = str(row['view_id']).split(',')
        ids = [x for x in ids if x != 'None' and x != '']

        orientation_list = str(row['orientation']).split(',')
        if len(orientation_list) != len(ids):
            orientation_list = orientation_list * len(ids)
        page_type_list = str(row['page_type']).split(',')
        if len(page_type_list) != len(ids):
            page_type_list = page_type_list * len(ids)

        project_filter_name = str(row['project_filter_name']).split(',')
        project_filter_name = [x for x in project_filter_name if x != 'None' and x != '']
        projects_list = str(row['projects_list']).split(';')
        projects_list = [x for x in projects_list if x != 'None' and x != '']
        project_first = row['project_first']
        apply_project_list = str(row['apply_project_list']).split(',')
        apply_project_list = [x for x in apply_project_list if x != 'None' and x != '']

        list_of_filters = str(row['list_of_filters']).split('/')
        if len(list_of_filters) != len(ids):
            list_of_filters = list_of_filters * len(ids)
        list_of_values = str(row['list_of_values']).split('/') * len(ids)
        if len(list_of_values) != len(ids):
            list_of_values = list_of_values * len(ids)

        in_charge_email = str(row['in_charge_email']).split(',')
        in_charge_email = [x for x in in_charge_email if x != 'None' and x != '']
        recipients_email = str(row['recipients_email']).split(',')
        recipients_email = [x for x in recipients_email if x != 'None' and x != '']
        body = str(row['body'])

        folder = str(row['folder'])

        is_test = row['is_test']
        link = str(row['link'])
## Рассылаем сложные отчеты
        if report_name[:21] == 'Аналитика по метрикам':
            month = int(str(list_of_values[0]).split(';')[1])
            year = int(str(list_of_values[0]).split(';')[0])
            special_sender.metrics_analytics_sender(report_name,
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
                                                    path=path)
        elif report_name[:7] == 'Summary':
            special_sender.summary_sender(ids[0],
                                          project_filter_name,
                                          projects_list,
                                          list_of_filters[0],
                                          list_of_values[0],
                                          chat_id,
                                          path=path)
## Рассылаем простые отчеты
        else:
            simple_sender.main(report_name,
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
                               path=path)
    #except:
     #   config.print("Сбой рассылки отчета "+str(row['report_name']))
