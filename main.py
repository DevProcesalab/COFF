# Inicializar librerias
from flask import Flask
from collections import Counter
from facebook_business.adobjects.offlineconversiondataset import OfflineConversionDataSet
from facebook_business.api import FacebookAdsApi
from numpy import False_, NaN, isnan

class FacebookService:
    def __init__(self):
        # Inicializar datos y token de Facebook
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
            ftp = FTP(host)
            ftp.login(user,password)
            # print('Conexion establecida')
        
        except Exception as e:
            # print('Conexión Errada: '+str(e))

        #cambiar carpeta
        ftp.cwd('Mercadeo_Ventasoff')


        ftp.dir(ftp.pwd(),lines.append)
        yesterday = date.today() - timedelta(days=1)
        # calcular la fecha de hoy
        fecha_hoy = pd.to_datetime('today').strftime('%m-%d-%y')
        
        for line in lines:
            tokens = line.split(maxsplit = 9)
            listTemp = tokens[3].split('_')
            cuenta_temp = listTemp.pop()
            if((tokens[0]==fecha_hoy) & (cuenta_temp=='GLX.csv')):
                dataArray['upload_tag']=tokens[3]
                files.append(tokens[3])
                r = BytesIO()
                ftp.retrbinary('RETR /Mercadeo_Ventasoff/'+tokens[3], r.write)
                data = str(r.getvalue())
                datatemp = data.split('\\n')

                cont=0
                total=len(datatemp)-1
                for row in datatemp:
                    if((cont>0) and (cont<total)):
                        
                        tempDateString = row.split('","')[13]
                        if(tempDateString==""):
                            tempDateString=yesterday.strftime('%d/%m/%Y')
                        
                        textemail=str(row.split('","')[0])
                        textphone=str(row.split('","')[1])
                        textfn=str(row.split('","')[3])
                        textln=str(row.split('","')[4])
                        fechaevento=datetime.datetime.strptime(tempDateString, "%d/%m/%Y").replace(tzinfo=datetime.timezone.utc).timestamp()
                        fechaevento1 = int(fechaevento)
                        
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


                return dataArray
                # Descomentar linea 109 para Imprimir información cargada 
                # print(dataArray)
                # Descomentar lineas 111,112 para Imprimir información en un json 
                # with open("test3.json","w")as f:
                #     json.dump(dataArray,f,indent=4)



        self.offline_dataset.create_event(params=dataArray)




app = Flask(__name__)


@app.route('/')
def hello():
    a=FacebookService()
    vista = a.upload_offline_conversion()
    return vista
    


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)

