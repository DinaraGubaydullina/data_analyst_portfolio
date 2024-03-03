from sqlalchemy import create_engine
from datetime import timedelta, date
import pandas as pd
import json

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import urllib

import math
import numpy as np

# допустим, у меня есть куча посадочных страниц для трекания и все они лежат в базе MS SQL в виде таблицы
# список может постоянно обновляться и дополняться
params = urllib.parse.quote_plus("DRIVER={SQL Server};"
                                "SERVER=DWH-SQL;"
                                "Database=DB;"
                                "Trusted_Connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

query = """
        select url
        from alerting_dict
        where department = 'media_advertising' and active = 1
        """

url = pd.read_sql(query, con=engine)
url_list = url['url'].to_list()

# допустим, основные данные лежат в clickhouse
# строим часть SQL запроса в clickhouse, работая со списком и f-строками
url_query = []
for url in url_list:
    url_query.append(f"`ym:s:startURL` like '%%{url}%%', '{url}'")
url_query = ", ".join(url_query)

# забираем данные из clickhouse
with open('data_click.json', 'r') as out:
    data = json.load(out)

uri = f"clickhouse://{data['login']}:{data['pass']}@clickhouse/default"
clickhouse_engine = create_engine(uri)

query = f"""
        select
            date,
            startURL,
            device_type,
            source_type,
            visitID,
            flag_fill_app,
            flag_lead
        from (
            select `ym:s:date` as date,
                    multiIf({url_query},
                    'N/A') as startURL,
                    multiIf(`ym:s:deviceCategory` = '1', 'desktop',
                   `ym:s:deviceCategory` in ('2', '3'), 'mobile+tablet',
                   'N/A') as device_type,
                   if(`ym:s:lastTrafficSource` in ('ad'), `ym:s:UTMMedium`, `ym:s:lastTrafficSource`) as source_type,
                   `ym:s:visitID` as visitID,
                   if(indexOf(`ym:s:goalsID`, 123) > 0, 1, 0) as flag_fill_app,
                   if(indexOf(`ym:s:goalsID`, 456) > 0, 1, 0) as flag_lead
            from DB.VISITS
            where `ym:s:date` between toDate(yesterday() - 7) and yesterday()
                and source_type in ('organic', 'context')
            order by 1, 2)
        where startURL != 'N/A' and device_type != 'N/A'
        """

df = pd.read_sql(query, con=clickhouse_engine)

check_date = date.today() - timedelta(days=1)
prev_date = date.today() - timedelta(days=8)
sigma_coef = 1
ma_window = 7

# алёрты только по URL

def result_df_url(metric):
    
    agg_data = df.groupby(['date', 'startURL']).agg({'visitID':'count', 'flag_fill_app': 'sum', 'flag_lead': 'sum'}).reset_index()
    agg_data.rename(columns={'visitID': 'Визиты', 'flag_fill_app': 'Приступили к заполнению заявки', 'flag_lead': 'Лиды'}, inplace=True)
    agg_data['date'] = pd.to_datetime(agg_data['date'])
    agg_data['date_upd'] = agg_data['date'].dt.date

    alerts = []
    results_df = pd.DataFrame(columns=['date', 'URL', metric, 'moving_average', 'deviation'])

    for url in agg_data['startURL'].unique():

        # moving average and st.dev (calculate based on last 7 days data)
        filtered_data_7d = agg_data[agg_data['startURL'] == url].copy()
        filtered_data_7d['ma'] = filtered_data_7d[metric].rolling(ma_window, closed='left').mean()
        std = filtered_data_7d[metric].std()

        # visits and moving average for a particular date
        filtered_data = agg_data[(agg_data['date_upd'] == check_date)&(agg_data['startURL'] == url)]
        if not filtered_data.empty:
            metric_item = filtered_data[metric].item()
            ma = filtered_data_7d[filtered_data_7d['date_upd'] == check_date]['ma'].item()
            
            if not np.isnan(ma - sigma_coef * std):
                if metric_item < math.floor(ma - sigma_coef * std):
                    lower_border = ma - sigma_coef * std
                    deviation = round((1 - metric_item / ma)*100, 2)
                    alerts.append((check_date, url, metric_item, ma, deviation))

    for alert in alerts:
        results_df = results_df.append({'date': alert[0],
                                        'URL': alert[1],
                                        metric: alert[2],
                                        'moving_average': alert[3],
                                        'deviation': alert[4]
                                        }, ignore_index=True)
        
    results_df.columns = ['Дата', 'URL', metric, 'Скользящее среднее за 7 дней', 'Отклонение от скользящего среднего, %']
    results_df.reset_index(drop=True, inplace=True)

    return results_df

# алёрты по URL + breakdown type (источник трафика или тип устройства)

def result_df_url_breakdown(metric, agg_type):
    
    agg_data = df.groupby(['date', 'startURL', agg_type]).agg({'visitID':'count', 'flag_fill_app': 'sum', 'flag_lead': 'sum'}).reset_index()
    agg_data.rename(columns={'visitID': 'Визиты', 'flag_fill_app': 'Приступили к заполнению заявки', 'flag_lead': 'Лиды'}, inplace=True)
    agg_data['date'] = pd.to_datetime(agg_data['date'])
    agg_data['date_upd'] = agg_data['date'].dt.date
    
    alerts = []
    results_df = pd.DataFrame(columns=['date', 'URL', agg_type, metric, 'moving_average', 'deviation'])
    
    for url, breakdown_type in agg_data[['startURL', agg_type]].drop_duplicates().itertuples(index=False):
        
        # moving average and st.dev (calculate based on last 7 days data)
        filtered_data_7d = agg_data[(agg_data['startURL'] == url)&(agg_data[agg_type] == breakdown_type)].copy()
        filtered_data_7d['ma'] = filtered_data_7d[metric].rolling(ma_window, closed='left').mean()
        std = filtered_data_7d[metric].std()
        
        # visits and moving average for a particular date
        filtered_data = agg_data[(agg_data['date_upd'] == check_date)&(agg_data['startURL'] == url)&(agg_data[agg_type] == breakdown_type)]
        if not filtered_data.empty:
            metric_item = filtered_data[metric].item()
            ma = filtered_data_7d[filtered_data_7d['date_upd'] == check_date]['ma'].item()
            
            if not np.isnan(ma - sigma_coef * std):
                if metric_item < math.floor(ma - sigma_coef * std):
                    lower_border = ma - sigma_coef * std
                    deviation = round((1 - metric_item / ma)*100, 2)
                    alerts.append((check_date, url, breakdown_type, metric_item, ma, deviation))
                
    for alert in alerts:
        results_df = results_df.append({'date': alert[0],
                                        'URL': alert[1],
                                        agg_type: alert[2], 
                                        metric: alert[3],
                                        'moving_average': alert[4],
                                        'deviation': alert[5]
                                        }, ignore_index=True)
        
    if agg_type == 'device_type':
        results_df.columns = ['Дата', 'URL', 'Тип устройства', metric, 'Скользящее среднее за 7 дней', 'Отклонение от скользящего среднего, %']
    elif agg_type == 'source_type':
        results_df.columns = ['Дата', 'URL', 'Источник трафика', metric, 'Скользящее среднее за 7 дней', 'Отклонение от скользящего среднего, %']

    results_df.reset_index(drop=True, inplace=True)

    return results_df

# форматируем для более красивого вида таблицы в Outlook
def style_df(results_df):
    
    def highlight_deviation(val):
        if isinstance(val, (int, float)):
            if val > 60:
                return 'background-color: #EA555E; color: white; font-weight: bold'
        return ''
    
    
    results_df_styled = results_df.style \
    .format(precision=2) \
    .set_properties(padding='10px', border='1px solid lightgrey', width='150px') \
    .set_properties(**{'background-color':'white'}) \
    .hide_index() \
    .applymap(highlight_deviation, subset=['Отклонение от скользящего среднего, %'])

    results_df_styled = results_df_styled.set_table_styles([
    {'selector': 'th.col_heading', 'props': 'text-align: center; background-color: #003791; font-family: helvetica; font-size: 14px; font-weight: bold; color: white'},
    {'selector': 'td', 'props': 'text-align: left; font-family: helvetica; font-size: 14px; '},], overwrite=False)
    
    return results_df_styled

# send alert
def send_alert(metric):
    
    strFrom = 'noreply@example.ru'
    strTo = 'data_analyst@example.ru'

    # create email message
    msg = MIMEMultipart()
    msg['Subject'] = f'[alert] {metric}'
    msg['From'] = strFrom
    msg['To'] = strTo

    # attach DataFrame as plain text
    result_url = result_df_url(metric)
        
    result_url_device = result_df_url_breakdown(metric, 'device_type')

    result_url_source = result_df_url_breakdown(metric, 'source_type')
    
    if metric == 'Визиты':
        
        part = ""
        
        if not result_url.empty:
            result_url_styled = style_df(result_url)
            part += f"""
            <span style="font-size: 14px; font-family: Helvetica, sans-serif;">
            <p> По URL </p>
            </span>
            {result_url_styled.to_html()}
            <br>
            """
        if not result_url_device.empty:
            result_url_device_styled = style_df(result_url_device)
            part += f"""
            <span style="font-size: 14px; font-family: Helvetica, sans-serif;">
            <p> По URL + тип устройства </p>
            </span>
            {result_url_device_styled.to_html()}
            <br>
            """
        if not result_url_source.empty:
            result_url_source_styled = style_df(result_url_source)
            part += f"""
            <span style="font-size: 14px; font-family: Helvetica, sans-serif;">
            <p> По URL + источник трафика </p>
            </span>
            {result_url_source_styled.to_html()}
            <br>
            """
        if part:    
            part += f"""
            <span style="font-size: 14px; font-family: Helvetica, sans-serif;">
            <p> Данные в таблицах отображаются за вчерашний день. Трекаем количество визитов. </p>
            <p> В разрезе: </p>
            <p> - посадочных страниц (по URL) </p>
            <p> - посадочных страниц + типа устройства (desktop, мобильные устройства + планшеты) </p>
            <p> - посадочных страниц + источника трафика </p>
            <p> Алёрт срабатывает, когда значение метрики отклонилось от скользящего среднего (то есть среднего значения метрики за последние 7 дней) более чем на 1 сигму (или 1 стандартное отклонение). </p>
            <p> В таблице можно увидеть % отклонение значения метрики от среднего. Оно подсветится красным, если отклонение более 60%. </p>
            </span>
            """
        
    elif metric == 'Приступили к заполнению заявки':
        
        part = ""
        
        if not result_url.empty:
            result_url_styled = style_df(result_url)
            part += f"""
            <span style="font-size: 14px; font-family: Helvetica, sans-serif;">
            <p> По URL </p>
            </span>
            {result_url_styled.to_html()}
            <br>
            """
        if not result_url_device.empty:
            result_url_device_styled = style_df(result_url_device)
            part += f"""
            <span style="font-size: 14px; font-family: Helvetica, sans-serif;">
            <p> По URL + тип устройства </p>
            </span>
            {result_url_device_styled.to_html()}
            <br>
            """
        if not result_url_source.empty:
            result_url_source_styled = style_df(result_url_source)
            part += f"""
            <span style="font-size: 14px; font-family: Helvetica, sans-serif;">
            <p> По URL + источник трафика </p>
            </span>
            {result_url_source_styled.to_html()}
            <br>
            """
        if part:
            part += f"""
            <span style="font-size: 14px; font-family: Helvetica, sans-serif;">
            <p> Данные в таблицах отображаются за вчерашний день. Трекаем количество приступивших к заполнению заявки. </p>
            <p> В разрезе: </p>
            <p> - посадочных страниц (по URL) </p>
            <p> - посадочных страниц + типа устройства (desktop, мобильные устройства + планшеты) </p>
            <p> - посадочных страниц + источника трафика </p>
            <p> Алёрт срабатывает, когда значение метрики отклонилось от скользящего среднего (то есть среднего значения метрики за последние 7 дней) более чем на 1 сигму (или 1 стандартное отклонение). </p>
            <p> В таблице можно увидеть % отклонение значения метрики от среднего. Оно подсветится красным, если отклонение более 60%. </p>
            </span>
            """
        
    elif metric == 'Лиды':
        
        part = ""
        
        if not result_url.empty:
            result_url_styled = style_df(result_url)
            part += f"""
            <span style="font-size: 14px; font-family: Helvetica, sans-serif;">
            <p> По URL </p>
            </span>
            {result_url_styled.to_html()}
            <br>
            """
        if not result_url_device.empty:
            result_url_device_styled = style_df(result_url_device)
            part += f"""
            <span style="font-size: 14px; font-family: Helvetica, sans-serif;">
            <p> По URL + тип устройства </p>
            </span>
            {result_url_device_styled.to_html()}
            <br>
            """
        if not result_url_source.empty:
            result_url_source_styled = style_df(result_url_source)
            part += f"""
            <span style="font-size: 14px; font-family: Helvetica, sans-serif;">
            <p> По URL + источник трафика </p>
            </span>
            {result_url_source_styled.to_html()}
            <br>
            """
        if part:
            part += f"""
            <span style="font-size: 14px; font-family: Helvetica, sans-serif;">
            <p> Данные в таблицах отображаются за вчерашний день. Трекаем количество лидов. </p> </p>
            <p> В разрезе: </p>
            <p> - посадочных страниц (по URL) </p>
            <p> - посадочных страниц + типа устройства (desktop, мобильные устройства + планшеты) </p>
            <p> - посадочных страниц + источника трафика </p>
            <p> Алёрт срабатывает, когда значение метрики отклонилось от скользящего среднего (то есть среднего значения метрики за последние 7 дней) более чем на 1 сигму (или 1 стандартное отклонение). </p>
            <p> В таблице можно увидеть % отклонение значения метрики от среднего. Оно подсветится красным, если отклонение более 60%. </p>
            </span>
            """
    
    if part:
        msg.attach(MIMEText(part, 'html'))

        with smtplib.SMTP(host='mail-int.example.group', port=00) as server:
            server.send_message(msg)

metrics = ['Визиты', 'Приступили к заполнению заявки', 'Лиды']
for metric in metrics:
    send_alert(metric)