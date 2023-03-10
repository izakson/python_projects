*Все конфиденциальные цифры, показатели и упоминания, относящиеся к компании, удалены или зашифрованы*


# Панель для рассылки отчетов

Это проект разработки инструмента, который рассылает на почту и в Телеграмм отчеты из Tableau или напрямую из базы данных, с приминением кастомных фильтров,
параметров, форматов документов согласно заданной периодичности. Потребность в данном инструменте возникала, т.к. пользователям не хватало 
функций встроенного рассылщика в Tableau.

## Возможности
1. Выбрать метод получения - почта или Телеграмм
2. Настроить периодичтность отправки - в определенное время каждую n-ую неделю во опр.день недели или каждый n-ый месяц в определенный день
3. Указать максимальное количество часов, прошедших с тех пор, как экстракт был обновлен в последний раз
4. Настроить ориентацию и формат листа
5. Указать необходимые фильтры и их значения
6. Задать генерацию одного и того же листа по определенному фильтру - указать номера листов, которые нужно сгенерировать, название фильтра и список значений для генерации.
7. Задать адреса почты ответственных за отчет и получателей
8. Вписать сообщение, которое будет приходить вместе с отчетом
9. Указать канал в Телеграмме, куда выслать отчет
10. Указать папку в корп.сети, куда положить отчет

## Скрипты:
1. [Файл для конфигураций, настроек и справочных материалов](https://github.com/izakson/python_projects/blob/main/report_sender/config%20(1).py)
2. [Скрипт для рассылки простых отчетов](https://github.com/izakson/python_projects/blob/main/report_sender/simple_sender.py)
3. [Скрипт для рассылки сложных отчетов](https://github.com/izakson/python_projects/blob/main/report_sender/special_sender.py)
4. [Скрипт для обновления файла со всеми дашбордами на сервере (ручник)](https://github.com/izakson/python_projects/blob/main/report_sender/view_updater.py)
5. [Главный скрипт, получающий данные из всех предыдущих и запускающий программу](https://github.com/izakson/python_projects/blob/main/report_sender/main.py)

Также добавлен [скриншот GS-файла (пользовательский интерфейс) со списком настроек](https://github.com/izakson/python_projects/tree/main/report_sender/%D0%9F%D0%BE%D0%BB%D1%8C%D0%B7%D0%BE%D0%B2%D0%B0%D1%82%D0%B5%D0%BB%D1%8C%D1%81%D0%BA%D0%B8%D0%B8%CC%86%20%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D1%84%D0%B5%D0%B8%CC%86%D1%81)

## Использованные библиотеки

- Для логирования: *logging, sys*
- Для работы с БД: *urllib, pyodbc, sqlalchemy*  
- Для работы с API Google Sheets: *oauth2client, httplib2, apiclient*
- Для работы с Tableau: *tableauserverclient, tableau_api_lib
- Для работы с файловой системой: *os*
- Для работы с pdf-файлоами: *PyPDF2, fpdf*
- Для обработки данных: *pandas, datetime, time*
- Для выгрузки результатов: *email.mime, smtplib* 

