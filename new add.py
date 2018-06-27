from arcgis.gis import GIS
from numpy import *
from arcpy import da
from pandas import *
from arcgis import geometry
from copy import deepcopy
from datetime import date, timedelta, datetime
import configparser
def pars_conf():
    global portal, log, password,weatherinput,analysinput,wealdinput,weatherdataid,analysdataid,wealddataid# обявлениее переменных для парсера
    parser = configparser.ConfigParser() #создание экземпляра парсера
    parser.read('settings.cfg')# чтёние конфига
    portal = parser.get('portal', 'portal')# присвоение url
    log = parser.get('portal', 'log')# присвоение логина
    password = parser.get('portal', 'password')# присвоение пароля
    weatherdataid = parser.get('portal','weatherdataid')
    analysdataid = parser.get('portal','analysdataid')
    wealddataid = parser.get('portal','wealddataid')
    weatherinput = parser.get('gdbpaths','weatherdata')

"""на данный момент заметил косяк, которого раньше не было. До этого в запрос с онлайн БД попадало ограниченное кол-во строк,(2000 по дефолту)
сейчас приходят все, что там есть, проблемма в том, что он в пустую прогоняет и старые строки, обновлений на которые уже нет, на что тратится уйма времени и скаждым днём затраты времени будут расти вместе с увеличением 
кол-ва данных"""
def log_p():
    global query, gis, weatherD#
    gis = GIS(portal, log, password)#логин в онлайне
    weatherdata = gis.content.get(weatherdataid)# пордключение по сервисайди к слою weather_data
    weatherD = weatherdata.layers[0]# выбор слоя из сервиса
    query = weatherD.query()#запрос данных из слоя
    onlinedf = query.df#конвертация в датафрейм
    #print(onlinedf)

def localGDB():#
    global local
    fields = ['OBJECTID', 'lon', 'lat', 'city_id', 'temp', 'forecast_date2', 'temp_min', 'temp_max',# объявление полей для чтения
              'pressure', 'pressure_s_lvl', 'pressure_g_lvl', 'wind_speed', 'wind_degree',# объявление полей для чтения
              'clouds', 'weather_description', 'humidity', 'request_date', 'forecast_date',# объявление полей для чтения
              'rain', 'snow', 'name']  # объявление полей для чтения
    fc_np = da.FeatureClassToNumPyArray(weatherinput, fields, skip_nulls=False,null_value=0)  # запрос данных из таблицы в нампай массив #where_clause=exp,
    localdf = DataFrame(fc_np)  # конвертация массива в датафрейм
    localdf.drop_duplicates(subset='forecast_date2', keep='last', inplace=True)
    print(localdf.shape)# избавление датафрейма от дубликатов
    local = localdf
    print(local)

    return local
def updating_feature():
    overlap_rows = pandas.merge(left=query.df, right=local, how='inner',
                                on='forecast_date2')
    print(overlap_rows)# связывание онлайн и локальной таблицы по "дата прогноза 2"
    features_for_update = []  # объявление списка для наполнения обновляемыми строками
    all_features = query.features  #создание шаблона обновляемой строки
    i = 0  #
    for forecast_date2 in overlap_rows['forecast_date2']:  #
         features_for_update = []
         original_feature = [f for f in all_features if f.attributes['forecast_date2'] == forecast_date2][0]  #Получение строки которую надо обновить
         feature_to_be_updated = deepcopy(original_feature)  #
         matching_row = local.where(local.forecast_date2 == forecast_date2).dropna()  #Вытаскивание из фрейма совпадающей строки
         feature_to_be_updated.attributes['temp'] = float(matching_row['temp'])


         feature_to_be_updated.attributes['temp_min'] = int(matching_row['temp_min'])  #Подмена значений в шаблонй строке на новую
         feature_to_be_updated.attributes['temp_max'] = int(matching_row['temp_max'])
         feature_to_be_updated.attributes['request_date'] = str(matching_row['request_date'].values[0])
         feature_to_be_updated.attributes['pressure'] = int(matching_row['pressure'])  #
         feature_to_be_updated.attributes['pressure_g_lvl'] = int(matching_row['pressure_g_lvl'])  #
         feature_to_be_updated.attributes['wind_speed'] = float(matching_row['wind_speed'])
         feature_to_be_updated.attributes['wind_degree'] = str((matching_row['wind_degree'].values[0]))#
         feature_to_be_updated.attributes['clouds'] = int(matching_row['clouds'])  #
         feature_to_be_updated.attributes['weather_description'] = str((matching_row['weather_description'].values[0]))
         feature_to_be_updated.attributes['rain'] = float(matching_row['rain'])  #
         feature_to_be_updated.attributes['snow'] = float(matching_row['snow'])  #
         feature_to_be_updated.attributes['forecast_date'] = str(matching_row['forecast_date'].values[0])
         features_for_update.append(feature_to_be_updated)#создание списка обновляемых строк
         i = i + 1
         print(i)
         print(features_for_update)
         print('====================================')
    weatherD.edit_features(updates=features_for_update)  #применение обновлений
    new_rows = local[~local['forecast_date2'].isin(overlap_rows['forecast_date2'])]
    print(new_rows)# идентификация строк отсутсвующих в онлайне
    features_to_be_added = [] #объявление списка строк, которые надо добавить
    template_feature = deepcopy(features_for_update[0])#копирование шаблона
    i = 0
    for row in new_rows.iterrows():#для каждой строки в фрейме создание строки
        i = i+1
        features_to_be_added = []
        new_feature = deepcopy(template_feature)

        print('creating {}'.format(i))#создание геометрии для новой строки, если залить без этой части, отображаться новые точки не будут
        input_geometry = {'y': float(row[1]['lat']),
                          'x': float(row[1]['lon'])}
        output_geometry = geometry.project(geometries=[input_geometry],
                                           in_sr=4326,
                                           out_sr=4326,#тут значение 4326 можно заменить на строку query.spatial_reference['latestWkid'],
                                           gis=gis)#в таком случае, он перестроит геометрию под онлайн слой, если она отличается от геометрии проекта, но если слой изначально пуст, то выйдет на ошибку

        new_feature.geometry = output_geometry[0]#запись геометрии в строку
        new_feature.attributes['lat']=float(row[1]['lat'])#Подстановка новых значений в отправляемую строку, поля широты и долготы в таком случае не особо актуальны, поскольку геометрия уже задана
        new_feature.attributes['lon']=float(row[1]['lon'])
        new_feature.attributes['city_id']=int(row[1]['city_id'])
        new_feature.attributes['temp']=float(row[1]['temp'])
        new_feature.attributes['temp_min']=int(row[1]['temp_min'])
        new_feature.attributes['temp_max']=int(row[1]['temp_max'])
        new_feature.attributes['pressure']=int(row[1]['pressure'])
        new_feature.attributes['pressure_s_lvl']=int(row[1]['pressure_s_lvl'])
        new_feature.attributes['pressure_g_lvl']=int(row[1]['pressure_g_lvl'])
        new_feature.attributes['wind_speed']=float(row[1]['wind_speed'])
        new_feature.attributes['forecast_date2']=str(row[1]['forecast_date2'])
        new_feature.attributes['clouds']=int(row[1]['clouds'])
        new_feature.attributes['request_date']=str(row[1]['request_date'])
        new_feature.attributes['forecast_date']=str(row[1]['forecast_date'])# время в данном случае отправляется в виде строки, и в онлайне автоматом разбивается на датувремя с учётом часовых поясов
        new_feature.attributes['rain']=float(row[1]['rain'])
        new_feature.attributes['snow']=float(row[1]['snow'])
        new_feature.attributes['wind_degree']=str(row[1]['wind_degree'])
        new_feature.attributes['name']=str(row[1]['name'])
        features_to_be_added.append(new_feature)#наполнение списка строками, которые нужно отправить
        print(features_to_be_added)
        print("============================================================")
        weatherD.edit_features(adds=features_to_be_added)# отправка

pars_conf()
log_p()
localGDB()
updating_feature()