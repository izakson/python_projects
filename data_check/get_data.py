# Выгружаем данные в нужном нам формате, производим обработку
import config

import sqlalchemy
import pandas as pd
import datetime




suffixes = {}  # Словарь для суффиксов, обозначающих отчеты, и их расшифровки
old_logs_list = []
new_logs_list = []
df_agg_list = []  # Список агрегированных датафреймов с выгрузками-детализациями каждого источника
df_month_agg_list = []  # Список агрегированных до отчетного месяца датафреймов каждого источника
list_date = []  # Список полей с датами в итоговой таблице
list_qty = []  # Список полей с количеством в итоговой таблице
list_sqr = []  # Список полей с площадью в итоговой таблице
list_rub = []  # Список полей с выручкой в итоговой таблице
list_compensation = []  # Список полей с компенсацией в итоговой таблице
list_cost_num = []
list_cost_denom = []


# -----------------------------------------------------------------------------------------------
# -------------------------------------------МЕТОДОЛОГИЯ-----------------------------------------
# -----------------------------------------------------------------------------------------------

def check_qty(series):
    # Функция, проверяющая поле с количеством по правилам:
    # 1. Операция "Продажа" - положительное число.
    # 2. Операция "Возврат" - отрицательное число.
    # 3. Операция "Корректировка" или "Корректировка компенсации" - 0.
    series = series.tolist()
    operation = series[0]
    qty = pd.to_numeric(series[1])
    if operation == 'Продажа' and qty <= 0 or \
            operation == 'Возврат' and qty >= 0 or \
            operation in ('Корректировка', 'Корректировка компенсации') and qty != 0:
        return True
    return False


def check_sqr(series):
    # Функция, проверяющая поле с площадью по правилам:
    # 1. Операция "Продажа" - положительное число.
    # 2. Операция "Возврат" - отрицательное число.
    # 3. Операция "Корректировка" - число по модулю строго меньше максимально установленного (см.выше).
    # 4. Операция "Корректировка компенсации" - 0.
    series = series.tolist()
    operation = series[0]
    sqr = pd.to_numeric(series[1])
    if operation == 'Продажа' and sqr <= 0 or \
            operation == 'Возврат' and sqr >= 0 or \
            operation == 'Корректировка' and abs(sqr) >= config.max_sqr_corrected or \
            operation == 'Корректировка компенсации' and sqr != 0:
        return True
    return False


def check_rub(series):
    # Функция, проверяющая поле с выручкой по правилам:
    # 1. Операция "Продажа" - положительное число.
    # 2. Операция "Возврат" - отрицательное число.
    # 3. Операция "Корректировка" - любое число.
    # 4. Операция "Корректировка компенсации" - 0.
    series = series.tolist()
    operation = series[0]
    rub = pd.to_numeric(series[1])
    if operation == 'Продажа' and rub <= 0 or \
            operation == 'Возврат' and rub >= 0 or \
            operation == 'Корректировка компенсации' and rub != 0:
        return True
    return False


def check_compensation(series):
    # Функция, проверяющая поле с компенсацией по правилам:
    # 1. Операция "Продажа" - 0 или положительное число.
    # 2. Операция "Возврат" - 0 или отрицательное число.
    # 3. Операция "Корректировка" - 0.
    # 4. Операция "Корректировка компенсации" - любое число.
    series = series.tolist()
    operation = series[0]
    compensation = pd.to_numeric(series[1])
    if operation == 'Продажа' and compensation < 0 or \
            operation == 'Возврат' and compensation > 0 or \
            operation == 'Корректировка' and compensation != 0:
        return True
    return False


def check_cost_num(series):
    # Функция, проверяющая поле с числителем цены (выручкой) по правилам:
    # 1. Операция "Продажа" - положительное число.
    # 2. Операция "Возврат" - 0.
    # 3. Операция "Корректировка" - 0.
    # 4. Операция "Корректировка компенсации" - 0.
    series = series.tolist()
    operation = series[0]
    cost_num = pd.to_numeric(series[1])
    if cost_num < 0 or \
            operation != 'Продажа' and cost_num != 0:
        return True
    return False


def check_cost_denom(series):
    # Функция, проверяющая поле со знаментелем цены (площадью) по правилам:
    # 1. Операция "Продажа" - положительное число.
    # 2. Операция "Возврат" - 0.
    # 3. Операция "Корректировка" - 0.
    # 4. Операция "Корректировка компенсации" - 0.
    series = series.tolist()
    operation = series[0]
    cost_denom = pd.to_numeric(series[1])
    if cost_denom < 0 or \
            operation != 'Продажа' and cost_denom != 0:
        return True
    return False


def error_type(series):
    series = series.tolist()
    mistake_type = ''
    if series[2]:
        operation = series[0]
        value = pd.to_numeric(series[1])
        if operation in ('Продажа', 'Возврат'):
            if value > 0:
                mistake_type = 'Положительное значение'
            elif value < 0:
                mistake_type = 'Отрицательное значение'
            else:
                mistake_type = 'Нулевое значение'
        else:
            mistake_type = 'Ненулевое значение в операции ' + operation
    return mistake_type


def sqr_error_type(series):
    series = series.tolist()
    mistake_type = ''
    if series[2]:
        operation = series[0]
        value = pd.to_numeric(series[1])
        if operation in ('Продажа', 'Возврат'):
            if value > 0:
                mistake_type = 'Положительное значение'
            elif value < 0:
                mistake_type = 'Отрицательное значение'
            else:
                mistake_type = 'Нулевое значение'
        elif operation == 'Корректировка компенсации':
            mistake_type = 'Ненулевое значение в операции ' + operation
        else:
            mistake_type = 'Значение ' + str(value) + ' м² больше ' + str(int(config.max_sqr_corrected)) + ' м²'
    return mistake_type


def cost_error_type(series):
    series = series.tolist()
    mistake_type = ''
    if series[2]:
        operation = series[0]
        if operation in ('Продажа'):
            mistake_type = 'Положительное значение'
        else:
            mistake_type = 'Ненулевое значение в операции ' + operation
    return mistake_type


# -----------------------------------------------------------------------------------------------
# -------------------------------------------ОБРАБОТКА-------------------------------------------
# -----------------------------------------------------------------------------------------------

def transform_detalization(df, comp_to_corr=False, vtb_sales=False):
    # Преобразуем типы данных, заполняем пропуски
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    if comp_to_corr:
        df['operation'] = df['operation'].replace('Компенсация', 'Корректировка')
    if vtb_sales:
        df['operation'] = df['operation'].replace('Продажа ВТБ', 'Продажа')
    df['qty'] = df['qty'].fillna(0)
    df['sqr'] = df['sqr'].fillna(0)
    df['rub'] = df['rub'].fillna(0)
    df['compensation'] = df['compensation'].fillna(0)
    df['cost_denom'] = df['cost_denom'].fillna(0)
    df['cost_num'] = df['cost_num'].fillna(0)
    return df


def analyse_detalization(df):
    # Добавляем столбцы с маркерами аномальных значений
    df['wrong_qty'] = df[['operation', 'qty']].apply(check_qty, axis=1)
    df['wrong_sqr'] = df[['operation', 'sqr']].apply(check_sqr, axis=1)
    df['wrong_rub'] = df[['operation', 'rub']].apply(check_rub, axis=1)
    df['wrong_compensation'] = df[['operation', 'compensation']].apply(check_compensation, axis=1)
    df['wrong_cost_denom'] = df[['operation', 'cost_denom']].apply(check_cost_denom, axis=1)
    df['wrong_cost_num'] = df[['operation', 'cost_num']].apply(check_cost_num, axis=1)

    # Добавляем столбцы с типом аномальных значений
    df['wrong_qty_type'] = df[['operation', 'qty', 'wrong_qty']].apply(error_type, axis=1)
    df['wrong_sqr_type'] = df[['operation', 'sqr', 'wrong_sqr']].apply(sqr_error_type, axis=1)
    df['wrong_rub_type'] = df[['operation', 'rub', 'wrong_rub']].apply(error_type, axis=1)
    df['wrong_compensation_type'] = df[['operation', 'compensation', 'wrong_compensation']].apply(error_type, axis=1)
    df['wrong_cost_denom_type'] = df[['operation', 'cost_denom', 'wrong_cost_denom']].apply(cost_error_type, axis=1)
    df['wrong_cost_num_type'] = df[['operation', 'cost_num', 'wrong_cost_num']].apply(cost_error_type, axis=1)

    # Добавляем столбец с маркером дубля для продаж и возвратов
    df['doubles'] = df.query("operation in ('Продажа', 'Возврат')"). \
        duplicated(config.ids)
    # Добавляем столбец с маркером дубля для корректировок
    df['doubles_c'] = df[df['operation'] == 'Корректировка']. \
        duplicated(config.ids_corrections)
    # Добавляем столбец с маркером дубля для корректировок компенсаций
    df['doubles_cc'] = df[df['operation'] == 'Корректировка компенсации'].duplicated(
        config.ids_compensations_corrections)
    return df


def create_agg(df):
    # Создаем сводную таблицу
    types = ['wrong_qty_type', 'wrong_sqr_type', 'wrong_rub_type', 'wrong_compensation_type',
             'wrong_cost_denom_type', 'wrong_cost_num_type']
    for type in types:
        df[type] = df.groupby(config.ids)[type].transform(lambda x: '. '.join(x))
    df = df.drop_duplicates()

    df_agg = df.groupby(by=config.ids).agg({'date': 'max',
                                            'doubles': 'sum',
                                            'doubles_c': 'sum',
                                            'doubles_cc': 'sum',
                                            'qty': 'sum',
                                            'sqr': 'sum',
                                            'rub': 'sum',
                                            'compensation': 'sum',
                                            'cost_num': 'sum',
                                            'cost_denom': 'sum',
                                            'wrong_qty': 'sum',
                                            'wrong_sqr': 'sum',
                                            'wrong_rub': 'sum',
                                            'wrong_compensation': 'sum',
                                            'wrong_cost_num': 'sum',
                                            'wrong_cost_denom': 'sum'

                                            }).reset_index()
    df_agg = df_agg.merge(df[config.ids + types], how='inner', on=config.ids)
    # Избавляемся от лишних полей
    df_agg['doubles'] = df_agg['doubles'] + df_agg['doubles_c'] + df_agg['doubles_cc']
    df_agg = df_agg.drop(['doubles_c', 'doubles_cc'], axis=1)

    # Функция инкремента
    incr_func = lambda x: 1 if x else 0

    # Применяем функцию инкремента к маркерам аномальных значений
    df_agg['wrong_qty'] = df_agg['wrong_qty'].apply(incr_func)
    df_agg['wrong_sqr'] = df_agg['wrong_sqr'].apply(incr_func)
    df_agg['wrong_rub'] = df_agg['wrong_rub'].apply(incr_func)
    df_agg['wrong_compensation'] = df_agg['wrong_compensation'].apply(incr_func)
    df_agg['wrong_cost_num'] = df_agg['wrong_cost_num'].apply(incr_func)
    df_agg['wrong_cost_denom'] = df_agg['wrong_cost_denom'].apply(incr_func)

    return df_agg


def update_logs(suffix, df_agg, path):
    # Обновляем логи
    try:
        df_log_old = pd.read_sql("select * from dbo.log_sales" + suffix, config.bigdatatest_conn)
        old_logs_list.append(df_log_old)
        print("Скачал старые логи " + suffixes[suffix])

        # Сохраняем новый срез в excel и в хранилище.
        df_log_new = df_agg[config.columns_log]
        df_log_new['datetime'] = datetime.datetime.today()

        df_log_new.to_excel(
            path + 'logs/log_sales' + suffix + '_' + str(datetime.date.today()) + '.xlsx')

        if not config.is_test:
            df_log_new.to_sql('log_sales' + suffix, config.bigdatatest_conn, schema='dbo', if_exists='replace',
                              chunksize=50000, dtype=config.log_dtype, index=False)
            print("log_sales" + suffix + " обновлена")
        new_logs_list.append(df_log_new)

    except:
        df_log_new = df_agg[config.columns_log]
        df_log_new['datetime'] = datetime.datetime.today()
        df_log_new.to_excel(
            path + 'logs/log_sales' + suffix + '_' + str(datetime.date.today()) + '.xlsx')
        if not config.is_test:
            df_log_new.to_sql('log_sales' + suffix, config.bigdatatest_conn, schema='dbo', if_exists='replace',
                              chunksize=50000, dtype=config.log_dtype, index=False)
            print("log_sales" + suffix + " обновлена")


def create_month_agg(suffix, df_agg):
    # Формируем агрегированный до месяца датафрейм (понадобится позже для сохранения статистики).
    df_agg['rep_period'] = df_agg['date'].apply(lambda x: x.replace(day=1))
    df_month_agg = df_agg.groupby(by='rep_period').agg({'date': 'count',
                                                        'doubles': 'sum',
                                                        'qty': 'sum',
                                                        'sqr': 'sum',
                                                        'rub': 'sum',
                                                        'compensation': 'sum',
                                                        'cost_num': 'sum',
                                                        'cost_denom': 'sum',
                                                        'wrong_qty': 'sum',
                                                        'wrong_sqr': 'sum',
                                                        'wrong_rub': 'sum',
                                                        'wrong_compensation': 'sum',
                                                        'wrong_cost_num': 'sum',
                                                        'wrong_cost_denom': 'sum'
                                                        })
    df_month_agg['source_name'] = suffixes[suffix]
    df_month_agg['lines'] = df_month_agg['date']
    df_month_agg = df_month_agg.reset_index()
    df_month_agg_list.append(df_month_agg)
    return df_month_agg


def transform_agg(suffix, df_agg):
    # Преобразовываем агрегированный датафрейм.
    df_agg = df_agg.drop('rep_period', axis=1)
    # Переименовываем столбцы (добавляем суффикс)
    columns = {'date': 'date' + suffix,
               'cid': 'cid',
               'oid': 'oid',
               'operation': 'operation',
               'qty': 'qty' + suffix,
               'sqr': 'sqr' + suffix,
               'rub': 'rub' + suffix,
               'compensation': 'compensation' + suffix,
               'cost_num': 'cost_num' + suffix,
               'cost_denom': 'cost_denom' + suffix,
               'doubles': 'doubles' + suffix,
               'wrong_qty': 'wrong_qty' + suffix,
               'wrong_sqr': 'wrong_sqr' + suffix,
               'wrong_rub': 'wrong_rub' + suffix,
               'wrong_compensation': 'wrong_compensation' + suffix,
               'wrong_cost_num': 'wrong_cost_num' + suffix,
               'wrong_cost_denom': 'wrong_cost_denom' + suffix,
               'wrong_qty_type': 'wrong_qty_type' + suffix,
               'wrong_sqr_type': 'wrong_sqr_type' + suffix,
               'wrong_rub_type': 'wrong_rub_type' + suffix,
               'wrong_compensation_type': 'wrong_compensation_type' + suffix,
               'wrong_cost_num_type': 'wrong_cost_num_type' + suffix,
               'wrong_cost_denom_type': 'wrong_cost_denom_type' + suffix
               }
    df_agg.rename(columns, axis='columns', inplace=True)
    return df_agg


def update_sales_analysis(suffix, df_agg):
    # Фиксируем поля в словаре полей и добавляем тип данных
    config.sales_analysis_dtype.update([('date' + suffix, sqlalchemy.DateTime()),
                                        ('doubles' + suffix, sqlalchemy.types.Integer),
                                        ('qty' + suffix, sqlalchemy.types.Integer),
                                        ('sqr' + suffix, sqlalchemy.types.Float),
                                        ('rub' + suffix, sqlalchemy.types.Float),
                                        ('compensation' + suffix, sqlalchemy.types.Float),
                                        ('cost_num' + suffix, sqlalchemy.types.Float),
                                        ('cost_denom' + suffix, sqlalchemy.types.Float),
                                        ('wrong_qty' + suffix, sqlalchemy.types.Boolean),
                                        ('wrong_sqr' + suffix, sqlalchemy.types.Boolean),
                                        ('wrong_rub' + suffix, sqlalchemy.types.Boolean),
                                        ('wrong_compensation' + suffix, sqlalchemy.types.Boolean),
                                        ('wrong_cost_num' + suffix, sqlalchemy.types.Boolean),
                                        ('wrong_cost_denom' + suffix, sqlalchemy.types.Boolean),
                                        ('wrong_qty_type' + suffix, sqlalchemy.types.NVARCHAR(500)),
                                        ('wrong_sqr_type' + suffix, sqlalchemy.types.NVARCHAR(500)),
                                        ('wrong_rub_type' + suffix, sqlalchemy.types.NVARCHAR(500)),
                                        ('wrong_compensation_type' + suffix, sqlalchemy.types.NVARCHAR(500)),
                                        ('wrong_cost_num_type' + suffix, sqlalchemy.types.NVARCHAR(500)),
                                        ('wrong_cost_denom_type' + suffix, sqlalchemy.types.NVARCHAR(500))
                                        ])

    # Добавляем в список датафреймов
    df_agg_list.append(df_agg)
    # Формируем списки полей для анализа
    list_date.append('date' + suffix)
    list_qty.append('qty' + suffix)
    list_qty.append('wrong_qty' + suffix)
    list_qty.append('doubles' + suffix)
    list_sqr.append('sqr' + suffix)
    list_sqr.append('wrong_sqr' + suffix)
    list_sqr.append('doubles' + suffix)
    list_rub.append('rub' + suffix)
    list_rub.append('wrong_rub' + suffix)
    list_rub.append('doubles' + suffix)
    list_compensation.append('compensation' + suffix)
    list_compensation.append('wrong_compensation' + suffix)
    list_compensation.append('doubles' + suffix)
    list_cost_num.append('cost_num' + suffix)
    list_cost_num.append('wrong_cost_num' + suffix)
    list_cost_num.append('doubles' + suffix)
    list_cost_denom.append('cost_denom' + suffix)
    list_cost_denom.append('wrong_cost_denom' + suffix)
    list_cost_denom.append('doubles' + suffix)


def main(source_name, suffix, engine, sql, path, comp_to_corr=False, vtb_sales=False):
    print("Начинаю загружать детализацию " + source_name)
    conn = engine.connect()
    print("Успешно подключился к " + source_name)
    suffixes.update({suffix: source_name})
    df = pd.read_sql(sql, conn)
    print("Скачал детализацию " + source_name)
    df = transform_detalization(df, comp_to_corr, vtb_sales)
    print("Преобразовал детализацию " + source_name)
    df = analyse_detalization(df)
    print("Проанализировал детализацию " + source_name)
    df_agg = create_agg(df)
    print("Создан агрегат " + source_name)
    update_logs(suffix, df_agg, path)
    df_month_agg = create_month_agg(suffix, df_agg)
    df_agg = transform_agg(suffix, df_agg)
    update_sales_analysis(suffix, df_agg)

