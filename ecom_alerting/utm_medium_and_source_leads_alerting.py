from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sqlalchemy import create_engine
import json

import pickle
import io
import pytz

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage


with open('data_mysql.json', 'r') as out:
    data = json.load(out)

db_connection_str = f"mysql://{data['login']}:{data['pass']}@00.00.000.000:0000/db"
engine_query = create_engine(db_connection_str)


# определим часовой пояс
timezone = 'Europe/Moscow'

# границы, по которым определяется используемое правило
# если пришло более 500 лидов за прошлые 24 часа, то будем трекать поступающее кол-во лидов каждые 10 минут
# если было от 100 до 500, то каждый час
# если менее 100, то сделаем суточную проверку
def rule(leads):
    if leads >= 500:
        return '10min'
    elif 100 <= leads < 500:
        return '1hour'
    else:
        return '1day'

# проводим проверку на количество лидов за последние 24 часа каждый час в :00 минут
if datetime.now().astimezone(pytz.timezone(timezone)).minute == 0:

    query_check = """
                SELECT utm_medium, utm_source, count(internet_application_id) as leads_previous_day
                FROM internet_applications
                WHERE 
                    utm_medium in ('affiliate', 'agent')
                    AND utm_source in (
                                    select distinct utm_source
                                    from internet_applications
                                    where created_date between curdate() - interval 30 day and curdate()
                                        and utm_medium in ('affiliate', 'agent')
                                        )
                    AND created_date between current_timestamp() - interval 24 hour and current_timestamp()
                GROUP BY 1, 2
                ORDER BY 1, 2
                """

    df_check = pd.read_sql(query_check, engine_query)
    df_check['rule'] = df_check['leads_previous_day'].apply(rule)
    
    # нужно будет указать путь, где будут сохраняться результаты проверок
    df_check.to_pickle('df_check.pkl')

# иначе просто берем результат прошлой проверки
else:
    df_check = pd.read_pickle('df_check.pkl')
    
# создаём таблицу при отклонении от нижней границы (10минутное правило)
def create_table_low(interval, leads_cnt, low):

    # создаём датафрейм для отправки
    data_to_send = pd.DataFrame([])

    # считаем отклонение от границы
    deviation = (low - leads_cnt) / low * 100

    # заполняем таблицу данными
    data_list = [[interval, leads_cnt, low, deviation]]
    data_to_send = data_to_send.append(data_list)

    data_to_send.columns = ['Время', 'Количество лидов', 'Нижняя граница', 'Отклонение в %']
    data_to_send.reset_index(drop=True, inplace=True)

    # это нужно для более красивого вида таблицы в Outlook
    data_to_send = data_to_send.style \
    .format(precision=2) \
    .set_properties(padding='10px', border='1px solid lightgrey', width='200px') \
    .set_properties(**{'background-color':'white'}) \
    .hide_index()

    data_to_send = data_to_send.set_table_styles([
    {'selector': 'th.col_heading', 'props': 'text-align: center; background-color: #F5F5F5; font-family: helvetica; font-size: 14px; font-weight: normal'},
    {'selector': 'td', 'props': 'text-align: center; font-family: helvetica; font-size: 14px; '},], overwrite=False)

    return data_to_send

# создаём таблицу при отклонении от верхней границы (10минутное правило)
def create_table_high(interval, leads_cnt, high):

    # создаём датафрейм для отправки
    data_to_send = pd.DataFrame([])

    # считаем отклонение от границы
    deviation = (leads_cnt - high) / high * 100

    # заполняем таблицу данными
    data_list = [[interval, leads_cnt, high, deviation]]
    data_to_send = data_to_send.append(data_list)

    data_to_send.columns = ['Время', 'Количество лидов', 'Верхняя граница', 'Отклонение в %']
    data_to_send.reset_index(drop=True, inplace=True)

    data_to_send = data_to_send.style \
    .format(precision=2) \
    .set_properties(padding='10px', border='1px solid lightgrey', width='200px') \
    .set_properties(**{'background-color':'white'}) \
    .hide_index()

    data_to_send = data_to_send.set_table_styles([
    {'selector': 'th.col_heading', 'props': 'text-align: center; background-color: #F5F5F5; font-family: helvetica; font-size: 14px; font-weight: normal'},
    {'selector': 'td', 'props': 'text-align: center; font-family: helvetica; font-size: 14px; '},], overwrite=False)

    return data_to_send

# функция, которая создает визуализацию для алёрта (10минутное правило, дневные часы)
def create_visualization(time, leads, low, high):
    
    fig, ax = plt.subplots(figsize=(15,5))

    ax.scatter(time, leads, color='crimson', label='количество лидов')

    ax.plot(time, low, color='lightseagreen', linestyle='dashed', label='нижняя граница')
    ax.plot(time, high, color='seagreen', linestyle='dashed', label='верхняя граница')

    plt.xticks(np.arange(0, len(time), 1))
    
    ax.set(xlabel='время', ylabel='лиды')
    
    plt.legend()
    
    return fig

# функция, которая отправляет таблицу и визуализацию в алёрте на почту (10минутное правило, дневные часы)
def send_email_10min(df, plot):

    # convert plot to virtual file
    buffer = io.BytesIO()
    plot.savefig(buffer, format='png')
    buffer.seek(0)
    
    strFrom = 'noreply@example.ru'
    strTo = 'data_analyst@example.ru'

    # create email message
    msg = MIMEMultipart()
    msg['Subject'] = f'Отклонение по лидам - {medium} {source} - 10min'
    msg['From'] = strFrom
    msg['To'] = strTo

    # attach DataFrame as plain text
    text = df.to_html()
    part = MIMEText(f'<br>{text}<br><br><img src="cid:image1"><br>', 'html')
    msg.attach(part)

    # attach plot as image
    img = MIMEImage(buffer.getvalue())
    img.add_header('Content-ID', '<image1>')
    msg.attach(img)

    with smtplib.SMTP(host='mail-int.example.group', port=00) as server:
        server.send_message(msg)

# функция, которая отправляет таблицу в алёрте на почту (10минутное правило, ночные часы)
def send_email_night(df):

    strFrom = 'noreply@example.ru'
    strTo = 'data_analyst@example.ru'

    # create email message
    msg = MIMEMultipart()
    msg['Subject'] = f'Отклонение по лидам - {medium} {source} - hourly'
    msg['From'] = strFrom
    msg['To'] = strTo

    # attach DataFrame as plain text
    text = df.to_html()

    part = MIMEText(f'<br>{text}<br>', 'html')
    msg.attach(part)
        
    with smtplib.SMTP(host='mail-int.example.group', port=00) as server:
        server.send_message(msg)

# функция, которая отправляет таблицу в алёрте на почту (часовое правило)
def send_email_1h(df_low, df_high, df_low_styled, df_high_styled):

    strFrom = 'noreply@example.ru'
    strTo = 'data_analyst@example.ru'

    # create email message
    msg = MIMEMultipart()
    msg['Subject'] = f'Отклонение по лидам - utm_medium + utm_source - hourly'
    msg['From'] = strFrom
    msg['To'] = strTo

    # attach DataFrame as plain text
    text_low = df_low_styled.to_html()
    text_high = df_high_styled.to_html()
    
    # если нет данных при отклонении от нижней границы, то отправляем только таблицу с данными об отклонении от верхней границы
    if len(df_low) == 0 and len(df_high) != 0:
        part = MIMEText(f'<br>{text_high}<br>', 'html')
        msg.attach(part)

    # если нет данных при отклонении от верхней границы, то отправляем только таблицу с данными об отклонении от нижней границы    
    elif len(df_high) == 0 and len(df_low) != 0:
        part = MIMEText(f'<br>{text_low}<br>', 'html')
        msg.attach(part)
    
    # или отправляем обе таблицы
    elif len(df_high) != 0 and len(df_low) != 0:
        part = MIMEText(f'<br>{text_low}<br><br>{text_high}<br>', 'html')
        msg.attach(part)
        
    with smtplib.SMTP(host='mail-int.example.group', port=00) as server:
        server.send_message(msg)
        
# функция, которая отправляет таблицу в алёрте на почту (суточное правило)
def send_email_1d(df_low, df_high, df_low_styled, df_high_styled):

    strFrom = 'noreply@example.ru'
    strTo = 'data_analyst@example.ru'

    # create email message
    msg = MIMEMultipart()
    msg['Subject'] = f'Отклонение по лидам - utm_medium + utm_source - daily'
    msg['From'] = strFrom
    msg['To'] = strTo

    # attach DataFrame as plain text
    text_low = df_low_styled.to_html()
    text_high = df_high_styled.to_html()
    
    # если нет данных при отклонении от нижней границы, то отправляем только таблицу с данными об отклонении от верхней границы
    if len(df_low) == 0 and len(df_high) != 0:
        part = MIMEText(f'<br>{text_high}<br>', 'html')
        msg.attach(part)

    # если нет данных при отклонении от верхней границы, то отправляем только таблицу с данными об отклонении от нижней границы        
    elif len(df_high) == 0 and len(df_low) != 0:
        part = MIMEText(f'<br>{text_low}<br>', 'html')
        msg.attach(part)
    
    # или отправляем обе таблицы
    elif len(df_high) != 0 and len(df_low) != 0:
        part = MIMEText(f'<br>{text_low}<br><br>{text_high}<br>', 'html')
        msg.attach(part)
        
    with smtplib.SMTP(host='mail-int.example.group', port=00) as server:
        server.send_message(msg)
        
# из numpy.datetime64 в datetime.time
def npdt_to_dt(d):
    d = datetime.strptime(np.datetime_as_string(d,unit='s'), '%Y-%m-%dT%H:%M:%S').time()
    return d

# создаём пустые датафреймы для отправки в часовом и суточном правиле
data_to_send_1d_low = pd.DataFrame([])
data_to_send_1d_high = pd.DataFrame([])
data_to_send_1h_low = pd.DataFrame([])
data_to_send_1h_high = pd.DataFrame([])

for medium, source in df_check[['utm_medium', 'utm_source']].itertuples(index=False):

    # каждый день в 8:00
    if datetime.now().astimezone(pytz.timezone(timezone)).hour == 8 and datetime.now().astimezone(pytz.timezone(timezone)).minute == 0:
        
        counter_1d = 0 # обнуляем счетчик на количество запусков скрипта для "суточного" правила
        
        # файл pickle со значением счетчика
        filename_1d = 'counter_1d.pkl'

        with open(filename_1d, 'wb') as fi:
            # сохраняем значение счетчика в файл
            pickle.dump(counter_1d, fi)
            
    else: # иначе загружаем результат предыдущего счетчика на количество запусков скрипта для "суточного" правила
        filename_1d = 'counter_1d.pkl'

        with open(filename_1d, 'rb') as fi:
            counter_1d = pickle.load(fi)
    
    # если сейчас :00 - начало часа
    if datetime.now().astimezone(pytz.timezone(timezone)).minute == 0:
        
        counter_1h = 0 # обнуляем счетчик на количество запусков скрипта для "часового" правила
        
        # файл pickle со значением счетчика
        filename_1h = 'counter_1h.pkl'

        with open(filename_1h, 'wb') as fi:
            # сохраняем значение счетчика в файл
            pickle.dump(counter_1h, fi)
            
    else: # иначе загружаем результат предыдущего счетчика на количество запусков скрипта для "часового" правила
        filename_1h = 'counter_1h.pkl'

        with open(filename_1h, 'rb') as fi:
            counter_1h = pickle.load(fi)
    
    # если скрипт должен отработать по 10минутному правилу    
    if df_check[(df_check['utm_medium'] == medium)&(df_check['utm_source'] == source)]['rule'].item() == '10min':
    
        # запрос для 10минутного алёрта
        query = f"""
                SELECT 
                    *, 
                    case when time_hour != 0 then concat(convert(time_hour-1, char), '-', convert(time_hour, char)) else '23-24' end as prev_hour_interval, 
                    avg(leads_cnt) over (order by datetime_interval rows between 6 preceding and 1 preceding) as moving_average,
                    (avg(leads_cnt) over (order by datetime_interval rows between 6 preceding and 1 preceding)) / 3 as low_day,
                    (avg(leads_cnt) over (order by datetime_interval rows between 6 preceding and 1 preceding)) * 2.5 as high_day, 
                    sum(leads_cnt) over (order by datetime_interval rows between 6 preceding and 1 preceding) as leads_previous_hour,
                    sum(leads_cnt) over (order by datetime_interval rows between 12 preceding and 7 preceding) as leads_2h_ago, 
                    (sum(leads_cnt) over (order by datetime_interval rows between 12 preceding and 7 preceding)) / 2 as low_night,
                    (sum(leads_cnt) over (order by datetime_interval rows between 12 preceding and 7 preceding)) * 2 as high_night,
                    concat(convert(date_format(datetime_interval_start,'%%H:%%i:%%s'), char), ' - ', convert(date_format(datetime_interval_end,'%%H:%%i:%%s'), char)) as time_interval
                FROM
                    (with recursive cte as (
                        SELECT addtime(convert((curdate() - interval 1 day), datetime), '22:00:00') as min_time
                        union all 
                        SELECT min_time + interval 10 minute 
                        FROM cte 
                        WHERE min_time < addtime(convert(curdate(), datetime), '23:59:59')), 
                    q2 as (
                        SELECT 
                            from_unixtime(unix_timestamp(leads_time) - unix_timestamp(leads_time) %% 600) as group_time, 
                            hour(from_unixtime(unix_timestamp(leads_time) - unix_timestamp(leads_time) %% 600)) as time_hour,
                            count(internet_application_id) as leads_cnt
                        FROM
                            (SELECT 
                                addtime(convert(created_date, datetime), created_time) as leads_time,
                                internet_application_id
                            FROM internet_applications
                            WHERE utm_medium in ('{medium}')
                                AND utm_source in ('{source}')
                                AND created_date between (curdate() - interval 1 day) and curdate()
                                AND (case when created_date = (curdate() - interval 1 day) then created_time between '22:00:00' and '23:59:59' else created_time between '0:00:00' and '23:59:59' end)
                            ORDER BY leads_time
                            ) as q1
                        GROUP BY group_time)

                    SELECT 
                        concat(convert(min_time, char), ' - ', convert(min_time + interval 9 minute + interval 59 second, char)) as datetime_interval,
                        min_time as datetime_interval_start,
                        min_time + interval 9 minute + interval 59 second as datetime_interval_end,
                        ifnull(time_hour, hour(min_time)) as time_hour,
                        ifnull(leads_cnt, 0) as leads_cnt

                    FROM cte
                    LEFT JOIN q2 on q2.group_time >= cte.min_time and q2.group_time < cte.min_time + interval 10 minute
                    ORDER BY datetime_interval) as q3 
                """

        df_10min = pd.read_sql(query, engine_query)

        # если рассматриваемый час (сейчас минус 10 минут назад) относится к промежутку от 8 до 22, 
        # то пусть работают правила "дневных" часов:
        if (datetime.now().astimezone(pytz.timezone(timezone)) - timedelta(minutes=10)).hour in np.arange(8,22):

            for i in range(60, 144): # от 60 до 144 - это индексы строк, соответствующие интервалам по 10 минут с 8:00 до 21:59 текущего дня

                    ### для таблицы ###

                # промежуток времени
                interval = df_10min['time_interval'][i]
                # фактическое количество лидов
                leads_cnt = df_10min['leads_cnt'][i]
                # верхняя граница
                high_day = df_10min['high_day'][i]
                # нижняя граница
                low_day = df_10min['low_day'][i]

                    ### для визуализации ###

                # промежуток времени
                visual_interval = df_10min['time_interval'][i-6:i+1]
                # фактическое количество лидов
                visual_leads_cnt = df_10min['leads_cnt'][i-6:i+1]
                # верхняя граница
                visual_high_day = df_10min['high_day'][i-6:i+1]
                # нижняя граница
                visual_low_day = df_10min['low_day'][i-6:i+1]

                # начало и окончание 10минутного интервала
                time_interval_start = npdt_to_dt(df_10min['datetime_interval_start'].values[i])
                time_interval_end = npdt_to_dt(df_10min['datetime_interval_end'].values[i])

                # время для проверки = текущее время на момент проверки минус 10 минут назад
                check_time = (datetime.now().astimezone(pytz.timezone(timezone)) - timedelta(minutes=10)).time()

                if (leads_cnt < low_day) and (high_day !=0 and low_day != 0 and leads_cnt != 0) and (time_interval_start <= check_time <= time_interval_end):
                    data_to_send = create_table_low(interval, leads_cnt, low_day)
                    plot_to_send = create_visualization(visual_interval, visual_leads_cnt, visual_low_day, visual_high_day)
                    send_email_10min(data_to_send, plot_to_send)

                elif (leads_cnt > high_day) and (high_day !=0 and low_day != 0 and leads_cnt != 0) and (time_interval_start <= check_time <= time_interval_end):
                    data_to_send = create_table_high(interval, leads_cnt, high_day)
                    plot_to_send = create_visualization(visual_interval, visual_leads_cnt, visual_low_day, visual_high_day)
                    send_email_10min(data_to_send, plot_to_send)           


        # если рассматриваемый час (сейчас минус 10 минут назад) относится к промежутку от 0 до 8 или с 22 до 23 (включительно), 
        # то пусть работают правила "ночных" часов:
        elif (datetime.now().astimezone(pytz.timezone(timezone)) - timedelta(minutes=10)).hour in np.arange(0,8):

            for i in range(12, 60, 6): # от 12 до 60 - это индексы строк, соответствующие интервалам по 10 минут с 0:00 до 7:59 текущего дня

                # час
                hour_night = df_10min['prev_hour_interval'][i]
                # час из таблицы для сверки с текущим временем
                time_hour = df_10min['time_hour'][i]
                # сумма за предыдущий час
                sum_prev_hour = df_10min['leads_previous_hour'][i]
                # нижняя граница
                low_night = df_10min['low_night'][i]
                # верхняя граница
                high_night = df_10min['high_night'][i]

                # час проверки
                check_hour = datetime.now().astimezone(pytz.timezone(timezone)).hour
                # минуты для проверки
                check_minutes = datetime.now().astimezone(pytz.timezone(timezone)).minute

                # если сумма лидов за предыдущий час меньше/больше границы
                # и если час проверки совпадает с рассматриваемым часом в датафрейме
                # и время соответствует началу часа (:00 минут)
                # то отправляем алёрт

                if sum_prev_hour < low_night and check_hour == time_hour and check_minutes == 0:

                    data_to_send = create_table_low(hour_night, sum_prev_hour, low_night)
                    send_email_night(data_to_send)

                elif sum_prev_hour > high_night and check_hour == time_hour and check_minutes == 0:

                    data_to_send = create_table_high(hour_night, sum_prev_hour, high_night)
                    send_email_night(data_to_send)

        elif (datetime.now().astimezone(pytz.timezone(timezone)) - timedelta(minutes=10)).hour in np.arange(22,24):

            for i in range(144, 156, 6): # от 144 до 156 - это индексы строк, соответствующие интервалам по 10 минут с 22:00 до 23:59 текущего дня

                # час
                hour_night = df_10min['prev_hour_interval'][i]
                # час из таблицы для сверки с текущим временем
                time_hour = df_10min['time_hour'][i]
                # сумма за предыдущий час
                sum_prev_hour = df_10min['leads_previous_hour'][i]
                # нижняя граница
                low_night = df_10min['low_night'][i]
                # верхняя граница
                high_night = df_10min['high_night'][i]

                # час проверки
                check_hour = datetime.now().astimezone(pytz.timezone(timezone)).hour
                # минуты для проверки
                check_minutes = datetime.now().astimezone(pytz.timezone(timezone)).minute

                # если сумма лидов за предыдущий час меньше/больше границы
                # и если час проверки совпадает с рассматриваемым часом в датафрейме
                # и время соответствует началу часа (:00 минут)
                # то отправляем алёрт

                if sum_prev_hour < low_night and check_hour == time_hour and check_minutes == 0:

                    data_to_send = create_table_low(hour_night, sum_prev_hour, low_night)
                    send_email_night(data_to_send)

                elif sum_prev_hour > high_night and check_hour == time_hour and check_minutes == 0:

                    data_to_send = create_table_high(hour_night, sum_prev_hour, high_night)
                    send_email_night(data_to_send)
                   
    # если скрипт должен отработать по "часовому" правилу и в этом часе он ещё не запускался (т.е. счётчик должен быть = 0)
    elif df_check[(df_check['utm_medium'] == medium)&(df_check['utm_source'] == source)]['rule'].item() == '1hour' and counter_1h == 0:

        query = f"""
                    SELECT *, leads_prev_hour / 2 as low, leads_prev_hour * 2 + 2 as high
                    FROM
                        (SELECT 
                            leads_hour,
                            concat(convert(leads_hour, char), '-', convert(leads_hour+1, char)) as time_interval, 
                            count(internet_application_id) as leads_cnt,
                            lag(count(internet_application_id), 1) over (order by leads_hour) as leads_prev_hour
                        FROM
                            (SELECT
                                internet_application_id, 
                                created_date, 
                                created_time, 
                                hour(created_time) as leads_hour
                            FROM internet_applications
                            WHERE 
                                utm_medium in ('{medium}')
                                AND utm_source in ('{source}')
                                AND created_date = CURDATE()
                            ) as q1
                        GROUP BY leads_hour
                        ) as q2
                    """

        df_1h = pd.read_sql(query, engine_query)

        # текущий час
        current_hour = datetime.now().astimezone(pytz.timezone(timezone)).hour
        # минуты
        current_minutes = datetime.now().astimezone(pytz.timezone(timezone)).minute

        # если есть данные за предыдущий час
        if len(df_1h[df_1h['leads_hour'] == current_hour - 1]) != 0:

                ### для таблицы ###

            # промежуток времени
            interval = df_1h[(df_1h['leads_hour'] == current_hour - 1)]['time_interval'].item()
            # сумма за рассматриваемый час
            leads_cnt = df_1h[(df_1h['leads_hour'] == current_hour - 1)]['leads_cnt'].item()
            # нижняя граница
            low = df_1h[(df_1h['leads_hour'] == current_hour - 1)]['low'].item()
            # верхняя граница
            high = df_1h[(df_1h['leads_hour'] == current_hour - 1)]['high'].item()

                ### для визуализации ###

            # промежуток времени
            visual_interval = df_1h['time_interval'][0:len(df_1h)]
            # фактическое количество лидов
            visual_leads = df_1h['leads_cnt'][0:len(df_1h)]
            # нижняя граница
            visual_low = df_1h['low'][0:len(df_1h)]
            # верхняя граница
            visual_high = df_1h['high'][0:len(df_1h)]

            # если сумма лидов за рассматриваемый час (прошлый час) выходит за нижнюю/верхнюю границу и сейчас :00 минут, то отправляем алёрт
            if leads_cnt < low and current_minutes == 0:

                # считаем отклонение от границы
                deviation = (low - leads_cnt) / low * 100
                
                # добавляем строку в таблицу
                data_list_low = [[medium, source, interval, leads_cnt, low, deviation]]
                data_to_send_1h_low = data_to_send_1h_low.append(data_list_low)

            elif leads_cnt > high and current_minutes == 0:

                # считаем отклонение от границы
                deviation = (leads_cnt - high) / high * 100
                
                # добавляем строку в таблицу
                data_list_high = [[medium, source, interval, leads_cnt, high, deviation]]
                data_to_send_1h_high = data_to_send_1h_high.append(data_list_high)

    # если скрипт должен отработать по суточному правилу и сегодня он ещё не запускался (т.е. счётчик должен быть = 0)
    elif df_check[(df_check['utm_medium'] == medium)&(df_check['utm_source'] == source)]['rule'].item() == '1day' and counter_1d == 0:

        query = f"""
                SELECT *, leads_prev_day / 1.5 as low, leads_prev_day * 1.5 as high
                FROM
                    (SELECT 
                        created_date, 
                        count(internet_application_id) as leads_cnt, 
                        lag(count(internet_application_id), 1) over (order by created_date) as leads_prev_day
                    FROM
                        (SELECT 
                            internet_application_id, 
                            created_date, 
                            created_time
                        FROM internet_applications
                        WHERE 
                            utm_medium in ('{medium}')
                            AND utm_source in ('{source}')
                            AND CASE WHEN WEEKDAY(CURDATE())+1 = 1 THEN created_date in (CURDATE() - INTERVAL 8 DAY, CURDATE() - INTERVAL 1 DAY)
                                WHEN WEEKDAY(CURDATE())+1 = 2 THEN created_date in (CURDATE() - INTERVAL 4 DAY, CURDATE() - INTERVAL 1 DAY)
                                WHEN WEEKDAY(CURDATE())+1 between 3 and 6 THEN created_date between CURDATE() - INTERVAL 2 DAY and CURDATE() - INTERVAL 1 DAY
                                WHEN WEEKDAY(CURDATE())+1 = 7 THEN created_date in (CURDATE() - INTERVAL 8 DAY, CURDATE() - INTERVAL 1 DAY) END) as q1
                    GROUP BY created_date) as q2
                """

        df_1d = pd.read_sql(query, engine_query)

        if len(df_1d) > 1:

            # проверяемый день - вчера
            check_day = date.today() - timedelta(days=1)

            # количество лидов за вчера
            leads_cnt = df_1d[df_1d['created_date'] == check_day]['leads_cnt'].item()

            # нижняя граница
            low = df_1d[df_1d['created_date'] == check_day]['low'].item()

            # верхняя граница
            high = df_1d[df_1d['created_date'] == check_day]['high'].item()
            
            # если сумма лидов за вчера выходит за нижнюю/верхнюю границу, то отправляем алёрт
            if leads_cnt < low:
            
                # считаем отклонение от границы
                deviation = (low - leads_cnt) / low * 100
                
                # добавляем строку в таблицу
                data_list_low = [[medium, source, check_day, leads_cnt, low, deviation]]
                data_to_send_1d_low = data_to_send_1d_low.append(data_list_low)

            elif leads_cnt > high:
            
                # считаем отклонение от границы
                deviation = (leads_cnt - high) / high * 100
                
                # добавляем строку в таблицу
                data_list_high = [[medium, source, check_day, leads_cnt, high, deviation]]
                data_to_send_1d_high = data_to_send_1d_high.append(data_list_high)

# Отправляем данные для суточного алёрта
# если есть данные для алёрта, то форматируем получившуюся таблицу
if len(data_to_send_1d_low) > 0:
    data_to_send_1d_low.columns = ['utm_medium', 'utm_source', 'День', 'Количество лидов за день', 'Нижняя граница', 'Отклонение в %']
    data_to_send_1d_low.reset_index(drop=True, inplace=True)
    data_to_send_1d_low.sort_values(by='Отклонение в %', ascending=False, inplace=True)

    data_to_send_1d_low_styled = data_to_send_1d_low.style \
    .format(precision=2) \
    .set_properties(padding='10px', border='1px solid lightgrey', width='200px') \
    .set_properties(**{'background-color':'white'}) \
    .hide_index()

    data_to_send_1d_low_styled = data_to_send_1d_low_styled.set_table_styles([
    {'selector': 'th.col_heading', 'props': 'text-align: center; background-color: #F5F5F5; font-family: helvetica; font-size: 14px; font-weight: normal'},
    {'selector': 'td', 'props': 'text-align: center; font-family: helvetica; font-size: 14px; '},], overwrite=False)

if len(data_to_send_1d_high) > 0:  
    data_to_send_1d_high.columns = ['utm_medium', 'utm_source', 'День', 'Количество лидов за день', 'Верхняя граница', 'Отклонение в %']
    data_to_send_1d_high.reset_index(drop=True, inplace=True)
    data_to_send_1d_high.sort_values(by='Отклонение в %', ascending=False, inplace=True)

    data_to_send_1d_high_styled = data_to_send_1d_high.style \
    .format(precision=2) \
    .set_properties(padding='10px', border='1px solid lightgrey', width='200px') \
    .set_properties(**{'background-color':'white'}) \
    .hide_index()

    data_to_send_1d_high_styled = data_to_send_1d_high_styled.set_table_styles([
    {'selector': 'th.col_heading', 'props': 'text-align: center; background-color: #F5F5F5; font-family: helvetica; font-size: 14px; font-weight: normal'},
    {'selector': 'td', 'props': 'text-align: center; font-family: helvetica; font-size: 14px; '},], overwrite=False)

# если счётчик равен 0, т.е. сегодня скрипт ещё не отрабатывал, и если есть какие-то данные для алёрта, то отправляем имейл
if counter_1d == 0 and (len(data_to_send_1d_low) != 0 or len(data_to_send_1d_high) != 0):    
    try:
        send_email_1d(data_to_send_1d_low, data_to_send_1d_high, data_to_send_1d_low_styled, data_to_send_1d_high_styled)

        counter_1d += 1 # так как скрипт отработал, то мы увеличиваем значение счетчика

        # открываем файл pickle со значением счетчика
        filename_1d = 'counter_1d.pkl'

        with open(filename_1d, 'wb') as fi:
            # сохраняем новое значение счетчика в файл
            pickle.dump(counter_1d, fi)
    except:
        print('Нет данных для отправки алёрта')

# Отправляем данные для часового алёрта
if len(data_to_send_1h_low) > 0:
    data_to_send_1h_low.columns = ['utm_medium', 'utm_source', 'Время', 'Количество лидов', 'Нижняя граница', 'Отклонение в %']
    data_to_send_1h_low.reset_index(drop=True, inplace=True)
    data_to_send_1h_low.sort_values(by='Отклонение в %', ascending=False, inplace=True)

    data_to_send_1h_low_styled = data_to_send_1h_low.style \
    .format(precision=2) \
    .set_properties(padding='10px', border='1px solid lightgrey', width='200px') \
    .set_properties(**{'background-color':'white'}) \
    .hide_index()

    data_to_send_1h_low_styled = data_to_send_1h_low_styled.set_table_styles([
    {'selector': 'th.col_heading', 'props': 'text-align: center; background-color: #F5F5F5; font-family: helvetica; font-size: 14px; font-weight: normal'},
    {'selector': 'td', 'props': 'text-align: center; font-family: helvetica; font-size: 14px; '},], overwrite=False)
    
if len(data_to_send_1h_high) > 0:  
    data_to_send_1h_high.columns = ['utm_medium', 'utm_source', 'Время', 'Количество лидов', 'Верхняя граница', 'Отклонение в %']
    data_to_send_1h_high.reset_index(drop=True, inplace=True)
    data_to_send_1h_high.sort_values(by='Отклонение в %', ascending=False, inplace=True)

    data_to_send_1h_high_styled = data_to_send_1h_high.style \
    .format(precision=2) \
    .set_properties(padding='10px', border='1px solid lightgrey', width='200px') \
    .set_properties(**{'background-color':'white'}) \
    .hide_index()

    data_to_send_1h_high_styled = data_to_send_1h_high_styled.set_table_styles([
    {'selector': 'th.col_heading', 'props': 'text-align: center; background-color: #F5F5F5; font-family: helvetica; font-size: 14px; font-weight: normal'},
    {'selector': 'td', 'props': 'text-align: center; font-family: helvetica; font-size: 14px; '},], overwrite=False)

if counter_1h == 0 and (len(data_to_send_1h_low) != 0 or len(data_to_send_1h_high) != 0):  
    try:  
        send_email_1h(data_to_send_1h_low, data_to_send_1h_high, data_to_send_1h_low_styled, data_to_send_1h_high_styled)

        counter_1h += 1 # так как скрипт отработал, то мы увеличиваем значение счетчика

        # открываем файл pickle со значением счетчика
        filename_1h = 'counter_1h.pkl'

        with open(filename_1h, 'wb') as fi:
            # сохраняем новое значение счетчика в файл
            pickle.dump(counter_1h, fi)
    except:
        print('Нет данных для отправки алёрта')