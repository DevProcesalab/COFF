# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START gae_python38_app]
# [START gae_python3_app]
from flask import Flask
from collections import Counter
from facebook_business.adobjects.offlineconversiondataset import OfflineConversionDataSet
from facebook_business.api import FacebookAdsApi
from numpy import False_, NaN, isnan

class FacebookService:
    def __init__(self):
        self.api = FacebookAdsApi.init(app_id='428798558259333', app_secret='758101ec30cecbfa9399700e9f1e1d5f',
        access_token='EAAGFZCXGZByIUBAIiaIMn3cICGizHsfNDEZCWb1q2LWKtx2SPrunBOpsnY8XDAMDauDU3MqGhaAPLkTA2WZAgsowjmZB5cQc04gaGfgrusQRcc3jMjauEGuZBCjpsp4HLUudToeMQ0ZAlr6gOgOHGmYHDSYC3u0WVh6SOhExLiRZCQZDZD')
        self.offline_dataset = OfflineConversionDataSet('624148101827847')
        
    def upload_offline_conversion(self):
        import pandas as pd
        from datetime import date, timedelta, datetime
        import time
        import numpy as np
        import json
        import hashlib
        import csv
        import datetime
        from ftplib import FTP
        from io import BytesIO
        files=[]
        lines=[]
        listTemp = []
        cuenta_temp = ''
        cuenta_temp_id = ''
        import os
        dataArray = {"upload_tag": "BASE.csv","data":[]}

        host = 'intfs.crystal.com.co'
        user  = 'ftpadbid'
        password = 'Crystal2021.*'
        #establecer conexion
        try:
            # ftp = FTP(host,user,password)
            ftp = FTP(host)
            ftp.login(user,password)
            print('Conexion establecida')
        
        except Exception as e:
            print('Conexión Errada: '+str(e))

        #cambiar carpeta
        ftp.cwd('Mercadeo_Ventasoff')
        print('Ahora estamos en la carpeta'+ftp.pwd()+'\n')


        ftp.dir(ftp.pwd(),lines.append)
        yesterday = date.today() - timedelta(days=1)
        # fecha_hoy = yesterday.strftime('%m-%d-%y')
        # fecha_hoy = pd.to_datetime('today').strftime('%m-%d-%y')
        fecha_hoy = '09-17-21'
        print(fecha_hoy)
        for line in lines:
            tokens = line.split(maxsplit = 9)
            # print(tokens[0]+" - "+tokens[3])
            listTemp = tokens[3].split('_')
            cuenta_temp = listTemp.pop()
            if((tokens[0]==fecha_hoy) & (cuenta_temp=='GLX.csv')):
                dataArray['upload_tag']=tokens[3]
                files.append(tokens[3])
                r = BytesIO()
                ftp.retrbinary('RETR /Mercadeo_Ventasoff/'+tokens[3], r.write)
                # print(r.getvalue())
                data = str(r.getvalue())
                datatemp = data.split('\\n')
                # print(datatemp)

                # cont=0
                # for row in datatemp:
                    # if(cont>1):
                    # print(row)
                    # print(row.split(',')[0])
                    # print("email:{0}, phone:{1} ,madid: {2}, fn : {3}, ln : {4}, zip : {5}, ct : {6}, st : {7}, currency : {8}, dob : {9}, doby : {9}, country : {10}, gen : {11}, age : {12}, event_time : {13}, value : {14} , event_name : {15}".format(row.split(',')[0],row.split(',')[1],row.split(',')[2],row.split(',')[3],row.split(',')[4],row.split(',')[5],row.split(',')[6],row.split(',')[7],row.split(',')[8],row.split(',')[9],row.split(',')[10],row.split(',')[11],row.split(',')[12],row.split(',')[13],row.split(',')[14],row.split(',')[15]))


        # print(files)
                cont=0
                total=len(datatemp)-1
                print(total)
                for row in datatemp:
                    if((cont>0) and (cont<total)):
                        # print(row.split(',')[13])
                        # tamanofecha=len(row.split(',')[13])
                        tempDateString = row.split('","')[13]
                        if(tempDateString==""):
                            tempDateString=yesterday.strftime('%d/%m/%Y')
                        
                        
                        # print(type(row.split('","')[0]))
                        
                        # print(time.mktime(datetime.datetime.strptime(tempDateString, "%d/%m/%Y").timetuple()))
                        textemail=str(row.split('","')[0])
                        textphone=str(row.split('","')[1])
                        textfn=str(row.split('","')[3])
                        textln=str(row.split('","')[4])
                        fechaevento=datetime.datetime.strptime(tempDateString, "%d/%m/%Y").replace(tzinfo=datetime.timezone.utc).timestamp()
                        fechaevento1 = int(fechaevento)
                        # print(fechaevento1,str(type(fechaevento1)))
                        # fechaevento2 = fechaevento.replace('.','')
                        dataArray["data"].append({
                            "match_keys": {
                                "email": hashlib.sha256(textemail.encode()).hexdigest(),
                                "phone": hashlib.sha256(textphone.encode()).hexdigest(),
                                "fn": hashlib.sha256(textfn.encode()).hexdigest(),
                                "ln": hashlib.sha256(textln.encode()).hexdigest()
                            },
                            "currency":"COP",
                            "value":str(row.split('","')[14]),
                            "event_name":row.split('","')[15],
                            "event_time":fechaevento1,
                            "custom_data":{
                                "fn":str(row.split('","')[3]).replace("\\",'"'),
                                "ln":str(row.split('","')[4]),
                                "ct":row.split('","')[6],
                                "st":row.split('","')[7],
                                "gen":row.split('","')[11],
                                "dob":row.split('","')[9],
                            }
                        })
                    
                    cont=cont+1
                    # print(cont)


                return dataArray
                # print(dataArray)
                # with open("test3.json","w")as f:
                #     json.dump(dataArray,f,indent=4)



        self.offline_dataset.create_event(params=dataArray)




# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)


@app.route('/')
def hello():
    a=FacebookService()
    vista = a.upload_offline_conversion()
    """Return a friendly HTTP greeting."""
    # return 'Hello World!'
    return vista
    


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python3_app]
# [END gae_python38_app]
