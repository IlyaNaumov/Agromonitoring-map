import configparser
from arcpy import da
import pandas as pd
from datetime import date, timedelta
def conf_parser():
    global weatherinput, s, analysinput
    parser = configparser.ConfigParser()
    parser.read('settings.cfg')
    weatherinput = parser.get('gdbpaths', 'weatherdata')
    analysinput = parser.get('gdbpaths', 'analysdata')
    s = parser.get('OWM', 's')
    s = s.split(',')
    pass
def anal():
    timeDF = (pd.DataFrame(da.FeatureClassToNumPyArray(analysinput, 'date', skip_nulls=False, null_value=0)).tail(1))
    starttime = pd.to_datetime(timeDF['date'].values[0])
    dtable = date((starttime.year), (starttime.month), (starttime.day))
    #d1 = date(2018, 5, 15)
    d1 = dtable+timedelta(days = 1)
    d2 = (date.today())
    delta=d2-d1

    fieldstoread = ['lat','lon','city_id',  'temp', 'clouds', 'weather_description',  'forecast_date2', 'forecast_date', 'rain',
             'snow', 'name']
    fieldstowrite = ['SHAPE@XY','name','id','rainsumm','snowsum','rainday','acttemp','avgtemp','clearday','date','t1','t2','t3','t4','t5']#
    for i in range(len(s)):
        print(s[i])
        exp = "city_id = {}".format(s[i])
        fc_np = da.FeatureClassToNumPyArray(weatherinput, fieldstoread, where_clause=exp, skip_nulls=False, null_value=0)
        local = pd.DataFrame(fc_np)
        local.drop_duplicates(subset='forecast_date2', keep='last', inplace=True)
        latlon = (float(local['lat'].values[0]), float(local['lon'].values[0]))
        print(type(latlon))
        print(latlon)
        for z in range(delta.days ):
            x = str(d1 + timedelta(days=z))
            rainsum = float(local.loc[local['forecast_date2'].str.contains('{}'.format(x))]['rain'].sum())
            snowsum = float(local.loc[local['forecast_date2'].str.contains('{}'.format(x))]['snow'].sum())
            acttempdf = (local.loc[local['forecast_date2'].str.contains('{}'.format(x))][local['temp']>10])
            avgtemp = float(local.loc[local['forecast_date2'].str.contains('{}'.format(x))]['temp'].mean())
            avgsun = float(local.loc[local['forecast_date2'].str.contains('{}'.format(x))]['clouds'].mean())
            name = (local['name'].values[0])
            t1df =  (local.loc[local['forecast_date2'].str.contains('{}'.format(x))][local['temp']>10])
            t2df =  (local.loc[local['forecast_date2'].str.contains('{}'.format(x))][local['temp']> 1])
            t3df = (local.loc[local['forecast_date2'].str.contains('{}'.format(x))][local['temp']> 2])
            t4df = (local.loc[local['forecast_date2'].str.contains('{}'.format(x))][local['temp']> 5])
            t5df = (local.loc[local['forecast_date2'].str.contains('{}'.format(x))][local['temp']> 8])
            id = int(s[i])
            if rainsum > 0:
                rainday = 1
            else:
                rainday = 0
            if avgsun < 16:
                clearday = 1
            else:
                clearday = 0
            if t1df.shape[0]<8:
                t1 = 0
            else:
                t1 = float(acttempdf['temp'].mean())
            if t2df.shape[0]<8:
                t2 = 0
            else:
                t2 = float(acttempdf['temp'].mean())
            if t3df.shape[0]<8:
                t3 = 0
            else:
                t3 = float(acttempdf['temp'].mean())
            if t4df.shape[0]<8:
                t4 = 0
            else:
                t4 = float(acttempdf['temp'].mean())
            if t5df.shape[0]<8:
                t5 = 0
            else:
                t5 = float(acttempdf['temp'].mean())
            if acttempdf.shape[0]<8:
                acttemp = 0
            else:

                acttemp=float(acttempdf['temp'].mean())
                pass

            cursor = da.InsertCursor(analysinput, fieldstowrite)
            print("creating row {} {} {} {} {} {} {}".format(latlon,name,id,rainsum,avgtemp,clearday,x))
            cursor.insertRow((latlon,name,id,rainsum,snowsum,rainday,acttemp,avgtemp,clearday,x,t1,t2,t3,t4,t5))#
            del cursor



conf_parser()
anal()