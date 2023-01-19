# Сравниваем датасеты между собой (проверка сходимости и неизменности)

import config
import datetime
import save_results

import pandas as pd
import sqlalchemy
import get_data


logs_changes_df_list = []
merged_logs_list = []

sales_analysis = []

# Функция, показывающая, что один из элементов Series пустой
def is_absent(list_of_dates):
    for date in list_of_dates:
        if pd.isnull(date):
            return True
    return False


# Функция, показывающая, что один из элементов Series отличается от других
def odd_one_out(list_of_numbers):
    list_of_numbers = list_of_numbers.tolist()
    # print(list_of_numbers)
    list_of_numbers_clean = []
    for n in range(len(list_of_numbers)):
        if n % 3 == 1:
            try:
                if not pd.isnull(list_of_numbers[n]) and list_of_numbers[n] == 0:
                    if list_of_numbers[n + 1] > 0:
                        list_of_numbers_clean.append(float(list_of_numbers[n - 1] / (list_of_numbers[n + 1] + 1), 2))
                    else:
                        list_of_numbers_clean.append(list_of_numbers[n - 1])
            except:
                pass
    # print(list_of_numbers_clean)
    if len(list_of_numbers_clean) > 0:
        value = sum(list_of_numbers_clean) / len(list_of_numbers_clean)
        # print(value)
        for number in list_of_numbers_clean:
            if round(number, 2) != round(value, 2):
                return True
    return False

# def discard_wrongs(sales_analysis, metric, first_suffix='_crm1'):
#     list_of_wrongs = []
#     list_of_metrics = []
#     df = sales_analysis
#     for suffix in list(get_data.suffixes.keys()):
#         list_of_wrongs.append(metric + suffix)
#         list_of_metrics.append(metric.split('_')[-1] + suffix)
#     df['sum_wrongs'] = df[list_of_wrongs].apply(sum,axis=1)
#     df = df[df['sum_wrongs']>1]
#     df['different_wrongs'] = df[list_of_metrics].apply(odd_one_out, axis=1)
#     df = df.query('not different_wrongs')
#     for wrong in list_of_wrongs:
#         if wrong.split('_')[-1] != first_suffix:
#             df[wrong] == 0
#         sales_analysis[wrong] = sales_analysis.merge(df,how='left', on=config.ids)[wrong]
#     merged_df = sales_analysis.merge(df,how='left', on=config.ids)
#


# Объединяем все датафреймы в один
def create_sales_analysis(df_agg_list):
    sales_analysis = df_agg_list[0]
    for i in range(len(df_agg_list) - 1):
        sales_analysis = sales_analysis.merge(df_agg_list[i + 1], how='outer', on=config.ids)
    print("Создан общий датафрейм")
    return sales_analysis

def analyse_sales(sales_analysis, list_date, list_qty, list_sqr, list_rub, list_compensation, list_cost_num, list_cost_denom):
    # Добавляем маркеры на отсутствие и расхождения
    sales_analysis['is_absent'] = sales_analysis[list_date].apply(is_absent, axis=1)
    sales_analysis['descr_qty'] = sales_analysis[list_qty].apply(odd_one_out, axis=1)
    sales_analysis['descr_sqr'] = sales_analysis[list_sqr].apply(odd_one_out, axis=1)
    sales_analysis['descr_rub'] = sales_analysis[list_rub].apply(odd_one_out, axis=1)
    sales_analysis['descr_compensation'] = sales_analysis[list_compensation].apply(odd_one_out, axis=1)
    sales_analysis['descr_cost_num'] = sales_analysis[list_cost_num].apply(odd_one_out, axis=1)
    sales_analysis['descr_cost_denom'] = sales_analysis[list_cost_denom].apply(odd_one_out, axis=1)
    # sales_analysis['descr_cost_num'] = sales_analysis['descr_cost_num']*(not sales_analysis['descr_cost_num'])
    # sales_analysis['descr_cost_denom'] = sales_analysis['descr_cost_denom']*(not sales_analysis['descr_cost_denom'])

    # Добавляем столбец с датой
    sales_analysis['date'] = datetime.datetime.today()
    return sales_analysis

def save_sales_analysis(sales_analysis, path):
    # Фиксируем новые поля в словаре полей и добавляем тип данных
    config.sales_analysis_dtype.update([('is_absent', sqlalchemy.types.Boolean),
                                 ('descr_qty', sqlalchemy.types.Boolean),
                                 ('descr_sqr', sqlalchemy.types.Boolean),
                                 ('descr_rub', sqlalchemy.types.Boolean),
                                 ('descr_compensation', sqlalchemy.types.Boolean),
                                        ('descr_cost_num', sqlalchemy.types.Boolean),
                                        ('descr_cost_denom', sqlalchemy.types.Boolean),
                                 ('date', sqlalchemy.DateTime())])

    # Сохраняем результат
    sales_analysis.to_excel(path)

    if not config.is_test:
        sales_analysis.to_sql('sales_analysis', config.bigdatatest_conn, schema='dbo', if_exists='replace',
                              chunksize=50000, dtype=config.sales_analysis_dtype)
        print("sales_analysis обновлена")


def is_workday(date):
    # Функция, определяющая, является ли дата рабочим днем
    if date.weekday() in (5, 6):
        return False
    if date.month == 1 and date.day in range(9) \
            or date.month == 2 and date.day == 23 \
            or date.month == 3 and date.day == 8 \
            or date.month == 5 and date.day in (1, 9) \
            or date.month == 6 and date.day == 12 \
            or date.month == 11 and date.day == 4:
        return False
    return True


def is_two_workdays_delta(date):
    # Функция, определяющая, входит ли дата в диапазон из первых двух рабочих дней с начала месяца
    first_day = date.replace(day=1)
    i = 0
    while i < 2:
        if is_workday(first_day):
            i += 1
        first_day += datetime.timedelta(days=1)
    if date < first_day:
        return True
    else:
        return False


MONTH_YESTERDAY = (datetime.date.today() - datetime.timedelta(days=1)).month
YEAR_YESTERDAY = (datetime.date.today() - datetime.timedelta(days=1)).year
DELTA_YESTERDAY = is_two_workdays_delta((datetime.date.today() - datetime.timedelta(days=1)))
PREVIOUS_MONTH_YESTERDAY = ((datetime.date.today() - datetime.timedelta(days=1)).replace(day=1)-datetime.timedelta(days=1)).month
PREVIOUS_YEAR_YESTERDAY = ((datetime.date.today() - datetime.timedelta(days=1)).replace(day=1)-datetime.timedelta(days=1)).year


def closed_period(list_of_dates):
    # Функция, определяющая, входит ли первая дата (или вторая дата, если первая - пустая) в закрытый отчетный период
    list_of_dates = list(list_of_dates)
    date = list_of_dates[0]
    if pd.isnull(date):
        date = list_of_dates[1]
    if date.month == MONTH_YESTERDAY and date.year == YEAR_YESTERDAY or \
            DELTA_YESTERDAY and date.month == PREVIOUS_MONTH_YESTERDAY and date.year == PREVIOUS_YEAR_YESTERDAY:
        return False
    return True


def merge_logs(old_logs, new_logs, source_name):
    merged_logs = old_logs.merge(new_logs, how='outer', on=config.ids, suffixes=('_old', '_new'))
    print("Склеил старые и новые данные отчета " + source_name)
    merged_logs['closed_period'] = merged_logs[['date_old', 'date_new']].apply(closed_period, axis=1)
    merged_logs_list.append(merged_logs)
    return merged_logs


def match_logs(merged_logs, logs_changes_dtype):
    try:
        merged_logs['is_gone'] = merged_logs[pd.isnull(merged_logs['date_old'])]
    except:
        merged_logs['is_gone'] = False
    try:
        merged_logs['is_new_date'] = \
            merged_logs[pd.isnull(merged_logs['date_old']) & merged_logs['closed_period']]['date_new']
    except:
        merged_logs['is_new_date'] = None
    try:
        merged_logs['changed_date'] = \
            merged_logs[pd.notnull(merged_logs['date_old'])].query('date_old != date_new and (closed_period or date_new < date_old)')['date_new']
    except:
        merged_logs['changed_date'] = None
    try:
        merged_logs['changed_doubles'] = \
            merged_logs[pd.notnull(merged_logs['date_old'])].query('doubles_old != doubles_new')['doubles_new']
    except:
        merged_logs['changed_doubles'] = None
    try:
        merged_logs['changed_qty'] = \
            merged_logs[pd.notnull(merged_logs['date_old'])].query('qty_old != qty_new')['qty_new']
    except:
        merged_logs['changed_qty'] = None
    try:
        merged_logs['changed_sqr'] = \
            merged_logs[pd.notnull(merged_logs['date_old'])].query('sqr_old != sqr_new')['sqr_new']
    except:
        merged_logs['changed_sqr'] = None
    try:
        merged_logs['changed_rub'] = \
            merged_logs[pd.notnull(merged_logs['date_old'])].query('rub_old != rub_new and closed_period')['rub_new']
    except:
        merged_logs['changed_rub'] = None
    try:
        merged_logs['changed_cost_num'] = \
             merged_logs[pd.notnull(merged_logs['date_old'])].query('cost_num_old != cost_num_new and closed_period and not changed_rub')['cost_num_new']
    except:
        merged_logs['changed_cost_num'] = None
    try:
        merged_logs['changed_cost_denom'] = \
            merged_logs[pd.notnull(merged_logs['date_old'])].query('cost_denom_old != cost_denom_new and not changed_sqr')['cost_denom_new']
    except:
        merged_logs['changed_cost_denom'] = None
    try:
        merged_logs['changed_compensation'] = merged_logs[pd.notnull(merged_logs['date_old'])].query(
            '(compensation_new != compensation_old) and closed_period')['compensation_new']
    except:
        merged_logs['changed_compensation'] = None

    logs_changes_df = pd.concat([merged_logs.query('is_gone')[list(logs_changes_dtype.keys())],
                                 merged_logs[pd.notnull(merged_logs['is_new_date'])][
                                     list(logs_changes_dtype.keys())],
                                 merged_logs[pd.notnull(merged_logs['changed_date'])][
                                     list(logs_changes_dtype.keys())],
                                 merged_logs[pd.notnull(merged_logs['changed_doubles'])][
                                     list(logs_changes_dtype.keys())],
                                 merged_logs[pd.notnull(merged_logs['changed_qty'])][
                                     list(logs_changes_dtype.keys())],
                                 merged_logs[pd.notnull(merged_logs['changed_sqr'])][
                                     list(logs_changes_dtype.keys())],
                                 merged_logs[pd.notnull(merged_logs['changed_rub'])][
                                     list(logs_changes_dtype.keys())],
                                 merged_logs[pd.notnull(merged_logs['changed_compensation'])][
                                     list(logs_changes_dtype.keys())]
                                 ,merged_logs[pd.notnull(merged_logs['changed_cost_num'])][
                                     list(logs_changes_dtype.keys())],
                                 merged_logs[pd.notnull(merged_logs['changed_cost_denom'])][
                                     list(logs_changes_dtype.keys())]
                                 ])
    print(logs_changes_df)
    print(logs_changes_df.info())
    return logs_changes_df

# def element_in_list(series):
#     element = str(series[0])
#     list_of_elements = str(series[1]).split(';')
#     return element in list_of_elements

# def change_is_fix(logs_changes_df, metric, sales_logs_changes):
#     metric_base = metric.split('_')[-1]
#     merged_logs_changes = logs_changes_df.merge(sales_logs_changes, how='left', on=config.ids, suffixes=('_now', '_then'))
#     merged_logs_changes.to_excel('C:/data_check/test.xlsx')
#     merged_logs_changes[metric_base+'_old'+'_then'] = merged_logs_changes[metric_base+'_old'+'_then'].astype(str)
#     merged_logs_changes['datetime_new_then'] = merged_logs_changes['datetime_new_then'].astype(str)
#     merged_logs_changes[metric_base+'_old'+'_then'] = merged_logs_changes.groupby(config.ids)[metric_base+'_old'+'_then'].transform(lambda x: ';'.join(x))
#     merged_logs_changes[metric_base + '_old' + '_then_date'] = merged_logs_changes.groupby(config.ids)['datetime_new_then'].transform(lambda x: ';'.join(x))
#     merged_logs_changes = merged_logs_changes.drop_duplicates()
#     merged_logs_changes['is_fix'] = merged_logs_changes[[metric+'_now',metric_base+'_old'+'_then']].apply(element_in_list, axis=1)
#     merged_logs_changes.to_excel('C:/data_check/merged_test.xlsx')


def save_changes(logs_changes_df, source_name, path):
    logs_changes_df['source_name'] = source_name
    logs_changes_df['datetime_new'] = datetime.datetime.today()
    config.logs_changes_dtype.update(
        {'source_name': sqlalchemy.types.NVARCHAR(100), 'datetime_new': sqlalchemy.DateTime()})
    logs_changes_df.to_excel(path + source_name + '. Изменения за ' + str(datetime.date.today()) + '.xlsx')
    save_results.attachments_logs.append(path + source_name + '. Изменения за ' + str(datetime.date.today()) + '.xlsx')
    if not config.is_test:
        logs_changes_df.to_sql('sales_logs_changes', config.bigdatatest_conn, schema='dbo', if_exists='append',
                               chunksize=50000, dtype=config.logs_changes_dtype, index=False)
    config.logs_changes_dtype.pop('source_name')
    config.logs_changes_dtype.pop('datetime_new')
    print(source_name + '. Изменения сохранены')


def main(path):
    global sales_analysis
    sales_analysis = create_sales_analysis(get_data.df_agg_list)
    analyse_sales(sales_analysis, get_data.list_date, get_data.list_qty, get_data.list_sqr, get_data.list_rub, get_data.list_compensation, get_data.list_cost_num, get_data.list_cost_denom)
    save_sales_analysis(sales_analysis, config.sales_analysis_xlsx)
    for c in range(len(get_data.old_logs_list)):
        source_name = list(get_data.suffixes.values())[c]
        merged_logs = merge_logs(get_data.old_logs_list[c], get_data.new_logs_list[c], source_name)
        logs_changes_df = match_logs(merged_logs, config.initial_logs_changes_dtype)
        print(logs_changes_df)
        writer=pd.ExcelWriter('logschanges.xlsx')
        logs_changes_df.to_excel(writer)
        writer.save()
        # if len(logs_changes_df):
        #     logs_changes_df_list.append(logs_changes_df)
        #     save_changes(logs_changes_df, source_name, path)


