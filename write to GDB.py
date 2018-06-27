import requests
import configparser
import datetime
import arcpy
def conf_parser():
    global s,appid,polyid,polyinput,weatherinput
    parser = configparser.ConfigParser()# создаёт экземпляр парсера
    parser.read('settings.cfg')#чтение файла конфига
    appid = parser.get('OWM', 'appid')#задаёт апиключ
    s = parser.get('OWM', 's')#задаёт список айди городов
    polyid = parser.get('OWM', 'polyid')

    polyinput = parser.get('gdbpaths','wealddata')
    s = s.split(',')#убирает из него запятые, можно задать список сразу без запятых, тогда строка не актуальна
    weatherinput = parser.get('gdbpaths','weatherdata')
    return

def wind_der(deg):
    x = ['С', 'СВ', 'В', 'ЮВ', 'Ю', 'ЮЗ', 'З', 'СЗ']#изменяет градусы на буквенные значения в направлении ветра
    for i in range(0, 8):#
        step = 45.
        min = i * step - 45 / 2.
        max = i * step + 45 / 2.
        if i == 0 and deg > 360 - 45 / 2.:
            deg = deg - 360
        if deg >= min and deg <= max:
            res = x[i]
            break
    return res
def agro_request():

    array = arcpy.Array([arcpy.Point(55.76317, 54.78371),
                         arcpy.Point(55.76737, 54.78505),
                         arcpy.Point(55.77072, 54.78148),
                         arcpy.Point(55.76634, 54.78025),
                         arcpy.Point(55.76317, 54.78371)])
    spatial_reference = arcpy.SpatialReference(4326)
    polygon = arcpy.Polygon(array, spatial_reference)
    polyfields = ['SHAPE@', 'name', 't10', 't0', 'moisture', 'id', 'dt']
    agror = requests.get('http://api.agromonitoring.com/agro/1.0/soil',
                         params={'polyid': polyid, 'appid': appid})
    cursor = arcpy.da.InsertCursor(polyinput, polyfields)
    data = agror.json()

    name = 'Миловка'
    t10 = float("{0:.2f}".format(data['t10'] - 273.15))
    t0 = float("{0:.2f}".format(data['t0'] - 273.15))
    moisture = float(data['moisture'])
    dt = datetime.datetime.fromtimestamp(data['dt'])
    print("Получение {} {}".format(name,dt))
    cursor.insertRow((polygon, name, t10, t0, moisture, polyid, dt))
def main():
    for i in range(len(s)):#запускает цикл для каждого айди города новый проход
        city_dict = {'Leninogorsk':'Лениногорск', 'Bugulma':'Бугульма', 'Bavly':'Бавлы', 'Almetyevsk':'Альметьевск',  'Menzelinsk':'Мензелинск',# переименовывет транслит в кириллицу для названий городов
                     'Kumertau' : 'Кумертау', 'Meleuz' : 'Мелеуз', 'Salavat': 'Салават', 'Abdulino': 'Абдулино', 'Priyutovo': 'Приютово', 'Belebey': 'Белебей', 'Oktyabrskiy':'Октябрьский',
                     'Tuymazy':'Туймазы', 'Agidel':'Агидель', 'Neftekamsk':'Нефтекамск', 'Rayevskiy': 'Раевский', 'Davlekanovo':'Давлеканово', 'Chishmy':'Чишмы',
                     'Ishimbay': 'Ишимбай', 'Sterlitamak':'Стеритамак', 'Ufa' : 'Уфа', 'Dyurtyuli':'Дюртюли', 'Birsk':'Бирск', 'Blagoveshchensk':'Благовещенск', 'Sarapul':'Сарапул',
                     'Chaykovskiy':'Чайковский', 'Yanaul':'Янаул','Chernushka':'Чернушка',  'Kuvandyk' : 'Кувандык ', 'Mednogorsk':'Медногорск' , 'Novotroitsk':'Новотроитск' ,
                     'Orsk':'Орск' , 'Gay' : 'Гай', 'Baymak' : 'Баймак' ,'Sibay' : 'Сибай', 'Magnitogorsk': 'Магнитогорск', 'Asha':'Аша', 'Katav-Ivanovsk':'Катав-Ивановск',
                     'Beloretsk' : 'Белорецк', 'Trekhgornyy': 'Трёхгорный', 'Sim':'Сим', 'Ust-Katav':'Усть-Катав','Bakal': 'Бакал', 'Satka':'Сатка',  'Uchaly':'Учалы', 'Zlatoust':'Златоуст',
                     'Kusa':' Куса', 'Miass':'Миасс', 'Chebarkul':'Чебаркуль', 'Karabash':'Карабаш', 'Verkhniy Ufaley':'Верхний Уфалей', 'Plast':'Пласт', 'Kyshtym':'Кыштым',  'Krasnoufimsk':'Красноуфимск',  'Polevskoy':'Полевской' }
        item = s[i]#присваивает в переменную текущий  для прохода айди города чтобы передачи его в запрос к апи
        try:

            res = requests.get("http://api.openweathermap.org/data/2.5/forecast",#запрос к апи
                               params={'id': item, 'units': 'metric', 'lang': 'ru', 'APPID': appid})# передача параметров
            data = res.json()# преобразует в json, не уверен, но вроде и без этой строки работает
            fields = ['SHAPE@XY','city_id', 'lat', 'lon', 'temp','temp_min', 'temp_max', 'pressure',#список полей таблицы в которые будем писать данные
                      'pressure_s_lvl','pressure_g_lvl','wind_speed', 'forecast_date2', 'clouds',
                      'weather_description', 'humidity', 'request_date', 'forecast_date', 'rain', 'snow', 'wind_degree', 'name']

            for i in data['list']:#запуск цикла для каждой строки(3х часового прогноза) из пришедшего пакета на 1 город
                rain = 0
                snow = 0
                cursor = arcpy.da.InsertCursor(weatherinput, fields)#создание экземпляра курсора, перенесу потом путь БД и поля в конфиг
                r_date = datetime.datetime.now()# присвоение текушего времени локальной машины для записи в "дата запроса"
                c_nameeng = str(data['city']['name'])#
                c_name = city_dict.get(c_nameeng)# дергает по транслитному ключу киррилицу
                c_id = str(data['city']['id'])
                latlon = ((data['city']['coord']['lon']), (data['city']['coord']['lat']))# latlon для записи в системное поле SHAPE@XY , без него не отображаются точки
                lon = (data['city']['coord']['lon'])
                lat = (data['city']['coord']['lat'])
                f_date = ((i['dt_txt'])[:16])#
                f_txt_date = (((i['dt_txt'])[:16])+ " " +str(data['city']['id']))#создаёт ключ к каждой строке ДАТА+АЙДИ ГОРОДА, чтобы при обновлении проверять прогноз на уникальность
                temp = (i['main']['temp'])
                temp_min = round(i['main']['temp_min'])
                temp_max = round(i['main']['temp_max'])
                pressure = round((i['main']['pressure'] * 10) //13)# значения давления приходят в килопаскалях, перевод в мм.рт.ст
                p_sea_lvl = round((i['main']['sea_level']) * 100 // 133)#
                p_grnd_lvl = round((i['main']['grnd_level']) * 100 // 133)#
                humidity = str(i['main']['humidity'])
                w_speed = float(i['wind']['speed'])
                w_degree = str(wind_der(i['wind']['deg']))
                clouds = str(i['clouds']['all'])
                w_description = str(i['weather'][0]['description'])
                print("Получение {} {}".format(c_name,f_date))
                try:
                    rain = (i['rain']['3h'])# если осадков нет, то ключ, в которым хранятся осадки в пришедшем пакете отсутсвует, приходится задавать исключение
                except Exception as x:

                    pass
                try:
                    snow = (i['snow']['3h'])
                except Exception as x:
                    pass

                cursor.insertRow(( latlon, c_id, lat, lon,  temp, temp_min, temp_max,#запись в таблицу
                                  pressure, p_sea_lvl, p_grnd_lvl, w_speed, f_txt_date, clouds, w_description,
                                  humidity, r_date,  f_date, rain, snow, w_degree, c_name))
            del cursor#удаление курсора
        except Exception as e:
              print("Exception (main):", e)
              pass

if __name__=='__main__':
    conf_parser()
    agro_request()
    main()