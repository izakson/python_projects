## Сводим все скрипты в один, запускаем главные функции.

import config
import get_data
import match_data
import save_results

if config.is_test:
    print("РЕЖИМ ТЕСТИРОВАНИЯ")

get_data.main('tableau.Продажи_Показатели (DWH.DWH2CRM)',
              '_dwh',
              config.dwh_engine,
              config.dwh_sales_sql,
              config.path)

get_data.main('map.fnGet_Sales (CRM2.ReportCRM)',
              '_crm2',
              config.crm2_engine,
              config.crm2_sales_sql,
              config.path,
              True, True)

match_data.main(config.path)

# save_results.main()
