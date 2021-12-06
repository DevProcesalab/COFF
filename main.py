from flask import Flask
from collections import Counter
from facebook_business.adobjects.offlineconversiondataset import OfflineConversionDataSet
from facebook_business.api import FacebookAdsApi
from numpy import False_, NaN, isnan

# Clase Principal de Python 
class FacebookService:
    # Función Contructora para inicializar variable y pixel offline Galax de Crystal
    def __init__(self):
        self.api = FacebookAdsApi.init(app_id='428798558259333', app_secret='758101ec30cecbfa9399700e9f1e1d5f',
        access_token='EAAGFZCXGZByIUBAK7BmflvcmZA05Y8B0rLxebZCz1UYLlNVfPSgdBUnnv7DRcZAi2r0lFgT6lNXzazFBDJUMZAVVHweKfdN6v8QSXVhUJH4rKNC33YzFr8xVgBWBDVInl8rqyXiTJAwVtFZBqgjnYIZCZBeFc7DCtiP1YPiPAWGCqCf37xJ1Xr4bUoeZC6ZAQZCERwHsOesM79019AZDZD')
        self.offline_dataset = OfflineConversionDataSet('624148101827847')
        
    # Función procesa y carga la data dias Lunes, Miercoles y Viernes
    def upload_offline_conversion(self):
        # Inicializar librerias
        import pandas as pd
        from datetime import date, timedelta, datetime
        import hashlib
        import datetime
        from ftplib import FTP
        from io import BytesIO
        import json
        # Inicializar variable y arreglos
        files=[]
        lines=[]
        listTemp = []
        cuenta_temp = ''
        # Inicializar arreglo principal
        
        dataArrayVista = {"upload_tag": "base.csv","data":[]}
        dataArray = {"upload_tag": "base.csv","data":[]}
        dataArray1 = {"upload_tag": "base.csv","data":[]}
        dataArray2 = {"upload_tag": "base.csv","data":[]}
        dataArray3 = {"upload_tag": "base.csv","data":[]}
        # Inicializar variables de conexion al servidor ftp Crystal
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

        #cambiar carpeta en donde se ubican los csv dentro del servidor
        ftp.cwd('Mercadeo_Ventasoff')
        print('Ahora estamos en la carpeta'+ftp.pwd()+'\n')

        # Agregar listado de archivos ubicados en servidor a lines[]
        ftp.dir(ftp.pwd(),lines.append)
        # Inicializar variables fechas de proceso
        yesterday = date.today() - timedelta(days=1)
        fecha_hoy = pd.to_datetime('today').strftime('%m-%d-%y')
        # fecha_hoy = '11-26-21'
 
        for line in lines:
            tokens = line.split(maxsplit = 9)
            # print(tokens[0]+" - "+tokens[3])
            # buscar extención de archivo GLX de la cuenta Galax de Crystal
            listTemp = tokens[3].split('_')
            cuenta_temp = listTemp.pop()

            # Si corresponde a extensión GLX y fecha hoy
            if((tokens[0]==fecha_hoy) & (cuenta_temp=='GLX.csv')):
                # asigna nombre del documento de hoy a upload_tag
                dataArrayVista["upload_tag"]=tokens[3]
                dataArray["upload_tag"]=tokens[3]
                dataArray1["upload_tag"]=tokens[3].split('.')[0]+"_1."+tokens[3].split('.')[1]
                dataArray2["upload_tag"]=tokens[3].split('.')[0]+"_2."+tokens[3].split('.')[1]
                dataArray3["upload_tag"]=tokens[3].split('.')[0]+"_3."+tokens[3].split('.')[1]

                # extrae datos directos del archivo de la cuenta de Galax de hoy
                files.append(tokens[3])
                r = BytesIO()
                ftp.retrbinary('RETR /Mercadeo_Ventasoff/'+tokens[3], r.write)
                
                # convierte bite en string
                data = str(r.getvalue().decode('utf8'))

                # separa datos por salto de linea
                datatemp = data.split('\n')
                # print(datatemp)

                cont=0
                cont1=0
                total=len(datatemp)-1
                # Recorre data
                if total <= 1500:
                    for row in datatemp:
                        if((cont>0) and (cont<total)):

                            TipoExtensionArray=1
                            textct=row.split('","')[6]
                            textst=row.split('","')[7]

                            textct=textct.replace("á", "a")
                            textct=textct.replace("é", "e")
                            textct=textct.replace("í", "i")
                            textct=textct.replace("ó", "o")
                            textct=textct.replace("ú", "u")
                            textct=textct.replace("ñ", "n")
                            textct=textct.replace("Á", "A")
                            textct=textct.replace("É", "E")
                            textct=textct.replace("Í", "I")
                            textct=textct.replace("Ó", "O")
                            textct=textct.replace("Ú", "U")
                            textct=textct.replace("Ñ", "N")

                            textst=textst.replace("á", "a")
                            textst=textst.replace("é", "e")
                            textst=textst.replace("í", "i")
                            textst=textst.replace("ó", "o")
                            textst=textst.replace("ú", "u")
                            textst=textst.replace("ñ", "n")
                            textst=textst.replace("Á", "A")
                            textst=textst.replace("É", "E")
                            textst=textst.replace("Í", "I")
                            textst=textst.replace("Ó", "O")
                            textst=textst.replace("Ú", "U")
                            textst=textst.replace("Ñ", "N")

                            # print("city",textct)
                            # print("state",textst)
                            
                            # corregir fechas de evento que vienen vacias
                            # print(row.split('","')[13])
                            tempDateString = row.split('","')[13]
                            if(tempDateString==""):
                                tempDateString=yesterday.strftime('%d/%m/%Y')

                            # validar fechas de cumpleaños 0-0-0
                            dobfinal = ""
                            tempdob = row.split('","')[9]
                            if (tempdob=="0-0-0"):
                                dobfinal = ""
                            else:
                                dobfinal=row.split('","')[9]
                            
                            # validar genero VACIO
                            genfinal = ""
                            gentemp = row.split('","')[11]
                            if (gentemp=="VACIO"):
                                genfinal = ""
                            else:
                                genfinal=row.split('","')[11]

                            # Replace
                            textfn=str(row.split('","')[3])
                            textln=str(row.split('","')[4])

                            textfn=textfn.replace("á", "a")
                            textfn=textfn.replace("é", "e")
                            textfn=textfn.replace("í", "i")
                            textfn=textfn.replace("ó", "o")
                            textfn=textfn.replace("ú", "u")
                            textfn=textfn.replace("ñ", "n")
                            textfn=textfn.replace("Á", "A")
                            textfn=textfn.replace("É", "E")
                            textfn=textfn.replace("Í", "I")
                            textfn=textfn.replace("Ó", "O")
                            textfn=textfn.replace("Ú", "U")
                            textfn=textfn.replace("Ñ", "N")

                            textln=textln.replace("á", "a")
                            textln=textln.replace("é", "e")
                            textln=textln.replace("í", "i")
                            textln=textln.replace("ó", "o")
                            textln=textln.replace("ú", "u")
                            textln=textln.replace("ñ", "n")
                            textln=textln.replace("Á", "A")
                            textln=textln.replace("É", "E")
                            textln=textln.replace("Í", "I")
                            textln=textln.replace("Ó", "O")
                            textln=textln.replace("Ú", "U")
                            textln=textln.replace("Ñ", "N")

                            # print(type(row.split('","')[0]))
                            # print(time.mktime(datetime.datetime.strptime(tempDateString, "%d/%m/%Y").timetuple()))
                            temp_email=str(row.split('","')[0])
                            textemail = temp_email[1:]
                            textphone=str(row.split('","')[1])

                            # print(textemail)
                            # fechaeventopru=datetime.datetime.strptime(tempDateString, "%d/%m/%Y")
                            # print(fechaeventopru)
                            fechaevento=datetime.datetime.strptime(tempDateString, "%d/%m/%Y").replace(tzinfo=datetime.timezone.utc).timestamp()
                            fechaevento1 = int(fechaevento)
                            # print(fechaevento1,str(type(fechaevento1)))
                            # fechaevento2 = fechaevento.replace('.','')

                            # opcion4
                            if ((textst=="") & (textst=="")):
                                textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                            elif(textct ==""):
                                textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"st":"'+hashlib.sha256(textst.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                            elif (textst==""):
                                textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"ct":"'+hashlib.sha256(textct.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                            else:
                                textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"ct":"'+hashlib.sha256(textct.encode()).hexdigest()+'",'+'"st":"'+hashlib.sha256(textst.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'

                            # remplazar backslahs
                            # print(textIni)
                            textIni=textIni.replace('\"','\'')

                            dataArray["data"].append(textIni)
                            dataArrayVista["data"].append(textIni)
                        
                        cont=cont+1
                        # print(cont)

                else:
                    if total >= 1500:
                        for row in datatemp:
                            if((cont1>0) and (cont1<=1500)):
                                TipoExtensionArray=1
                                textct=row.split('","')[6]
                                textst=row.split('","')[7]

                                textct=textct.replace("á", "a")
                                textct=textct.replace("é", "e")
                                textct=textct.replace("í", "i")
                                textct=textct.replace("ó", "o")
                                textct=textct.replace("ú", "u")
                                textct=textct.replace("ñ", "n")
                                textct=textct.replace("Á", "A")
                                textct=textct.replace("É", "E")
                                textct=textct.replace("Í", "I")
                                textct=textct.replace("Ó", "O")
                                textct=textct.replace("Ú", "U")
                                textct=textct.replace("Ñ", "N")

                                textst=textst.replace("á", "a")
                                textst=textst.replace("é", "e")
                                textst=textst.replace("í", "i")
                                textst=textst.replace("ó", "o")
                                textst=textst.replace("ú", "u")
                                textst=textst.replace("ñ", "n")
                                textst=textst.replace("Á", "A")
                                textst=textst.replace("É", "E")
                                textst=textst.replace("Í", "I")
                                textst=textst.replace("Ó", "O")
                                textst=textst.replace("Ú", "U")
                                textst=textst.replace("Ñ", "N")

                                # print("city",textct)
                                # print("state",textst)
                                
                                # corregir fechas de evento que vienen vacias
                                # print(row.split('","')[13])
                                tempDateString = row.split('","')[13]
                                if(tempDateString==""):
                                    tempDateString=yesterday.strftime('%d/%m/%Y')

                                # validar fechas de cumpleaños 0-0-0
                                dobfinal = ""
                                tempdob = row.split('","')[9]
                                if (tempdob=="0-0-0"):
                                    dobfinal = ""
                                else:
                                    dobfinal=row.split('","')[9]
                                
                                # validar genero VACIO
                                genfinal = ""
                                gentemp = row.split('","')[11]
                                if (gentemp=="VACIO"):
                                    genfinal = ""
                                else:
                                    genfinal=row.split('","')[11]

                                # Replace
                                textfn=str(row.split('","')[3])
                                textln=str(row.split('","')[4])

                                textfn=textfn.replace("á", "a")
                                textfn=textfn.replace("é", "e")
                                textfn=textfn.replace("í", "i")
                                textfn=textfn.replace("ó", "o")
                                textfn=textfn.replace("ú", "u")
                                textfn=textfn.replace("ñ", "n")
                                textfn=textfn.replace("Á", "A")
                                textfn=textfn.replace("É", "E")
                                textfn=textfn.replace("Í", "I")
                                textfn=textfn.replace("Ó", "O")
                                textfn=textfn.replace("Ú", "U")
                                textfn=textfn.replace("Ñ", "N")

                                textln=textln.replace("á", "a")
                                textln=textln.replace("é", "e")
                                textln=textln.replace("í", "i")
                                textln=textln.replace("ó", "o")
                                textln=textln.replace("ú", "u")
                                textln=textln.replace("ñ", "n")
                                textln=textln.replace("Á", "A")
                                textln=textln.replace("É", "E")
                                textln=textln.replace("Í", "I")
                                textln=textln.replace("Ó", "O")
                                textln=textln.replace("Ú", "U")
                                textln=textln.replace("Ñ", "N")

                                # print(type(row.split('","')[0]))
                                # print(time.mktime(datetime.datetime.strptime(tempDateString, "%d/%m/%Y").timetuple()))
                                temp_email=str(row.split('","')[0])
                                textemail = temp_email[1:]
                                textphone=str(row.split('","')[1])

                                # print(textemail)
                                # fechaeventopru=datetime.datetime.strptime(tempDateString, "%d/%m/%Y")
                                # print(fechaeventopru)
                                fechaevento=datetime.datetime.strptime(tempDateString, "%d/%m/%Y").replace(tzinfo=datetime.timezone.utc).timestamp()
                                fechaevento1 = int(fechaevento)
                                # print(fechaevento1,str(type(fechaevento1)))
                                # fechaevento2 = fechaevento.replace('.','')

                                # opcion4
                                if ((textst=="") & (textst=="")):
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                                elif(textct ==""):
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"st":"'+hashlib.sha256(textst.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                                elif(textst==""):
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"ct":"'+hashlib.sha256(textct.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                                else:
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"ct":"'+hashlib.sha256(textct.encode()).hexdigest()+'",'+'"st":"'+hashlib.sha256(textst.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'

                                # remplazar backslahs
                                # print(textIni)
                                textIni=textIni.replace('\"','\'')

                                dataArray["data"].append(textIni)
                                dataArrayVista["data"].append(textIni)

                            elif((cont1>=1501) & (cont1<=3000) & (cont1<total)):
                                TipoExtensionArray=2
                                
                                textct=row.split('","')[6]
                                textst=row.split('","')[7]

                                textct=textct.replace("á", "a")
                                textct=textct.replace("é", "e")
                                textct=textct.replace("í", "i")
                                textct=textct.replace("ó", "o")
                                textct=textct.replace("ú", "u")
                                textct=textct.replace("ñ", "n")
                                textct=textct.replace("Á", "A")
                                textct=textct.replace("É", "E")
                                textct=textct.replace("Í", "I")
                                textct=textct.replace("Ó", "O")
                                textct=textct.replace("Ú", "U")
                                textct=textct.replace("Ñ", "N")

                                textst=textst.replace("á", "a")
                                textst=textst.replace("é", "e")
                                textst=textst.replace("í", "i")
                                textst=textst.replace("ó", "o")
                                textst=textst.replace("ú", "u")
                                textst=textst.replace("ñ", "n")
                                textst=textst.replace("Á", "A")
                                textst=textst.replace("É", "E")
                                textst=textst.replace("Í", "I")
                                textst=textst.replace("Ó", "O")
                                textst=textst.replace("Ú", "U")
                                textst=textst.replace("Ñ", "N")

                                # print("city",textct)
                                # print("state",textst)
                                
                                # corregir fechas de evento que vienen vacias
                                # print(row.split('","')[13])
                                tempDateString = row.split('","')[13]
                                if(tempDateString==""):
                                    tempDateString=yesterday.strftime('%d/%m/%Y')

                                # validar fechas de cumpleaños 0-0-0
                                dobfinal = ""
                                tempdob = row.split('","')[9]
                                if (tempdob=="0-0-0"):
                                    dobfinal = ""
                                else:
                                    dobfinal=row.split('","')[9]
                                
                                # validar genero VACIO
                                genfinal = ""
                                gentemp = row.split('","')[11]
                                if (gentemp=="VACIO"):
                                    genfinal = ""
                                else:
                                    genfinal=row.split('","')[11]

                                # Replace
                                textfn=str(row.split('","')[3])
                                textln=str(row.split('","')[4])

                                textfn=textfn.replace("á", "a")
                                textfn=textfn.replace("é", "e")
                                textfn=textfn.replace("í", "i")
                                textfn=textfn.replace("ó", "o")
                                textfn=textfn.replace("ú", "u")
                                textfn=textfn.replace("ñ", "n")
                                textfn=textfn.replace("Á", "A")
                                textfn=textfn.replace("É", "E")
                                textfn=textfn.replace("Í", "I")
                                textfn=textfn.replace("Ó", "O")
                                textfn=textfn.replace("Ú", "U")
                                textfn=textfn.replace("Ñ", "N")

                                textln=textln.replace("á", "a")
                                textln=textln.replace("é", "e")
                                textln=textln.replace("í", "i")
                                textln=textln.replace("ó", "o")
                                textln=textln.replace("ú", "u")
                                textln=textln.replace("ñ", "n")
                                textln=textln.replace("Á", "A")
                                textln=textln.replace("É", "E")
                                textln=textln.replace("Í", "I")
                                textln=textln.replace("Ó", "O")
                                textln=textln.replace("Ú", "U")
                                textln=textln.replace("Ñ", "N")

                                # print(type(row.split('","')[0]))
                                # print(time.mktime(datetime.datetime.strptime(tempDateString, "%d/%m/%Y").timetuple()))
                                temp_email=str(row.split('","')[0])
                                textemail = temp_email[1:]
                                textphone=str(row.split('","')[1])

                                # print(textemail)
                                # fechaeventopru=datetime.datetime.strptime(tempDateString, "%d/%m/%Y")
                                # print(fechaeventopru)
                                fechaevento=datetime.datetime.strptime(tempDateString, "%d/%m/%Y").replace(tzinfo=datetime.timezone.utc).timestamp()
                                fechaevento1 = int(fechaevento)
                                # print(fechaevento1,str(type(fechaevento1)))
                                # fechaevento2 = fechaevento.replace('.','')

                                # opcion4
                                if ((textst=="") & (textst=="")):
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                                elif(textct ==""):
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"st":"'+hashlib.sha256(textst.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                                elif(textst==""):
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"ct":"'+hashlib.sha256(textct.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                                else:
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"ct":"'+hashlib.sha256(textct.encode()).hexdigest()+'",'+'"st":"'+hashlib.sha256(textst.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'

                                # remplazar backslahs
                                # print(textIni)
                                textIni=textIni.replace('\"','\'')

                                dataArray1["data"].append(textIni)
                                dataArrayVista["data"].append(textIni)

                            elif((cont1>=3001) & (cont1<=4500) & (cont1<total)):
                                TipoExtensionArray=3
                                textct=row.split('","')[6]
                                textst=row.split('","')[7]

                                textct=textct.replace("á", "a")
                                textct=textct.replace("é", "e")
                                textct=textct.replace("í", "i")
                                textct=textct.replace("ó", "o")
                                textct=textct.replace("ú", "u")
                                textct=textct.replace("ñ", "n")
                                textct=textct.replace("Á", "A")
                                textct=textct.replace("É", "E")
                                textct=textct.replace("Í", "I")
                                textct=textct.replace("Ó", "O")
                                textct=textct.replace("Ú", "U")
                                textct=textct.replace("Ñ", "N")

                                textst=textst.replace("á", "a")
                                textst=textst.replace("é", "e")
                                textst=textst.replace("í", "i")
                                textst=textst.replace("ó", "o")
                                textst=textst.replace("ú", "u")
                                textst=textst.replace("ñ", "n")
                                textst=textst.replace("Á", "A")
                                textst=textst.replace("É", "E")
                                textst=textst.replace("Í", "I")
                                textst=textst.replace("Ó", "O")
                                textst=textst.replace("Ú", "U")
                                textst=textst.replace("Ñ", "N")

                                # print("city",textct)
                                # print("state",textst)
                                
                                # corregir fechas de evento que vienen vacias
                                # print(row.split('","')[13])
                                tempDateString = row.split('","')[13]
                                if(tempDateString==""):
                                    tempDateString=yesterday.strftime('%d/%m/%Y')

                                # validar fechas de cumpleaños 0-0-0
                                dobfinal = ""
                                tempdob = row.split('","')[9]
                                if (tempdob=="0-0-0"):
                                    dobfinal = ""
                                else:
                                    dobfinal=row.split('","')[9]
                                
                                # validar genero VACIO
                                genfinal = ""
                                gentemp = row.split('","')[11]
                                if (gentemp=="VACIO"):
                                    genfinal = ""
                                else:
                                    genfinal=row.split('","')[11]

                                # Replace
                                textfn=str(row.split('","')[3])
                                textln=str(row.split('","')[4])

                                textfn=textfn.replace("á", "a")
                                textfn=textfn.replace("é", "e")
                                textfn=textfn.replace("í", "i")
                                textfn=textfn.replace("ó", "o")
                                textfn=textfn.replace("ú", "u")
                                textfn=textfn.replace("ñ", "n")
                                textfn=textfn.replace("Á", "A")
                                textfn=textfn.replace("É", "E")
                                textfn=textfn.replace("Í", "I")
                                textfn=textfn.replace("Ó", "O")
                                textfn=textfn.replace("Ú", "U")
                                textfn=textfn.replace("Ñ", "N")

                                textln=textln.replace("á", "a")
                                textln=textln.replace("é", "e")
                                textln=textln.replace("í", "i")
                                textln=textln.replace("ó", "o")
                                textln=textln.replace("ú", "u")
                                textln=textln.replace("ñ", "n")
                                textln=textln.replace("Á", "A")
                                textln=textln.replace("É", "E")
                                textln=textln.replace("Í", "I")
                                textln=textln.replace("Ó", "O")
                                textln=textln.replace("Ú", "U")
                                textln=textln.replace("Ñ", "N")

                                # print(type(row.split('","')[0]))
                                # print(time.mktime(datetime.datetime.strptime(tempDateString, "%d/%m/%Y").timetuple()))
                                temp_email=str(row.split('","')[0])
                                textemail = temp_email[1:]
                                textphone=str(row.split('","')[1])

                                # print(textemail)
                                # fechaeventopru=datetime.datetime.strptime(tempDateString, "%d/%m/%Y")
                                # print(fechaeventopru)
                                fechaevento=datetime.datetime.strptime(tempDateString, "%d/%m/%Y").replace(tzinfo=datetime.timezone.utc).timestamp()
                                fechaevento1 = int(fechaevento)
                                # print(fechaevento1,str(type(fechaevento1)))
                                # fechaevento2 = fechaevento.replace('.','')

                                # opcion4
                                if ((textst=="") & (textst=="")):
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                                elif(textct ==""):
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"st":"'+hashlib.sha256(textst.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                                elif(textst==""):
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"ct":"'+hashlib.sha256(textct.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                                else:
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"ct":"'+hashlib.sha256(textct.encode()).hexdigest()+'",'+'"st":"'+hashlib.sha256(textst.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'

                                # remplazar backslahs
                                # print(textIni)
                                textIni=textIni.replace('\"','\'')

                                dataArray2["data"].append(textIni)
                                dataArrayVista["data"].append(textIni)

                            elif((cont1>=4601) and (cont1<=6000) & (cont1<total)):
                                TipoExtensionArray=4
                                textct=row.split('","')[6]
                                textst=row.split('","')[7]

                                textct=textct.replace("á", "a")
                                textct=textct.replace("é", "e")
                                textct=textct.replace("í", "i")
                                textct=textct.replace("ó", "o")
                                textct=textct.replace("ú", "u")
                                textct=textct.replace("ñ", "n")
                                textct=textct.replace("Á", "A")
                                textct=textct.replace("É", "E")
                                textct=textct.replace("Í", "I")
                                textct=textct.replace("Ó", "O")
                                textct=textct.replace("Ú", "U")
                                textct=textct.replace("Ñ", "N")

                                textst=textst.replace("á", "a")
                                textst=textst.replace("é", "e")
                                textst=textst.replace("í", "i")
                                textst=textst.replace("ó", "o")
                                textst=textst.replace("ú", "u")
                                textst=textst.replace("ñ", "n")
                                textst=textst.replace("Á", "A")
                                textst=textst.replace("É", "E")
                                textst=textst.replace("Í", "I")
                                textst=textst.replace("Ó", "O")
                                textst=textst.replace("Ú", "U")
                                textst=textst.replace("Ñ", "N")

                                # print("city",textct)
                                # print("state",textst)
                                
                                # corregir fechas de evento que vienen vacias
                                # print(row.split('","')[13])
                                tempDateString = row.split('","')[13]
                                if(tempDateString==""):
                                    tempDateString=yesterday.strftime('%d/%m/%Y')

                                # validar fechas de cumpleaños 0-0-0
                                dobfinal = ""
                                tempdob = row.split('","')[9]
                                if (tempdob=="0-0-0"):
                                    dobfinal = ""
                                else:
                                    dobfinal=row.split('","')[9]
                                
                                # validar genero VACIO
                                genfinal = ""
                                gentemp = row.split('","')[11]
                                if (gentemp=="VACIO"):
                                    genfinal = ""
                                else:
                                    genfinal=row.split('","')[11]

                                # Replace
                                textfn=str(row.split('","')[3])
                                textln=str(row.split('","')[4])

                                textfn=textfn.replace("á", "a")
                                textfn=textfn.replace("é", "e")
                                textfn=textfn.replace("í", "i")
                                textfn=textfn.replace("ó", "o")
                                textfn=textfn.replace("ú", "u")
                                textfn=textfn.replace("ñ", "n")
                                textfn=textfn.replace("Á", "A")
                                textfn=textfn.replace("É", "E")
                                textfn=textfn.replace("Í", "I")
                                textfn=textfn.replace("Ó", "O")
                                textfn=textfn.replace("Ú", "U")
                                textfn=textfn.replace("Ñ", "N")

                                textln=textln.replace("á", "a")
                                textln=textln.replace("é", "e")
                                textln=textln.replace("í", "i")
                                textln=textln.replace("ó", "o")
                                textln=textln.replace("ú", "u")
                                textln=textln.replace("ñ", "n")
                                textln=textln.replace("Á", "A")
                                textln=textln.replace("É", "E")
                                textln=textln.replace("Í", "I")
                                textln=textln.replace("Ó", "O")
                                textln=textln.replace("Ú", "U")
                                textln=textln.replace("Ñ", "N")

                                # print(type(row.split('","')[0]))
                                # print(time.mktime(datetime.datetime.strptime(tempDateString, "%d/%m/%Y").timetuple()))
                                temp_email=str(row.split('","')[0])
                                textemail = temp_email[1:]
                                textphone=str(row.split('","')[1])

                                # print(textemail)
                                # fechaeventopru=datetime.datetime.strptime(tempDateString, "%d/%m/%Y")
                                # print(fechaeventopru)
                                fechaevento=datetime.datetime.strptime(tempDateString, "%d/%m/%Y").replace(tzinfo=datetime.timezone.utc).timestamp()
                                fechaevento1 = int(fechaevento)
                                # print(fechaevento1,str(type(fechaevento1)))
                                # fechaevento2 = fechaevento.replace('.','')

                                # opcion4
                                if ((textst=="") & (textst=="")):
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                                elif(textct ==""):
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"st":"'+hashlib.sha256(textst.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                                elif(textst==""):
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"ct":"'+hashlib.sha256(textct.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'
                                else:
                                    textIni='{match_keys: {'+'"phone":["'+hashlib.sha256(textphone.encode()).hexdigest()+'"],'+'"email":["'+hashlib.sha256(textemail.encode()).hexdigest()+'"],'+'"fn":"'+hashlib.sha256(textfn.encode()).hexdigest()+'",'+'"ln":"'+hashlib.sha256(textln.encode()).hexdigest()+'",'+'"ct":"'+hashlib.sha256(textct.encode()).hexdigest()+'",'+'"st":"'+hashlib.sha256(textst.encode()).hexdigest()+'",'+'"gen":"'+hashlib.sha256(row.split('","')[11].encode()).hexdigest()+'",'+'"dob":"'+hashlib.sha256(dobfinal.encode()).hexdigest()+'"'+'},'+'currency:"COP",'+'value:'+str(row.split('","')[14])+','+'event_name:"'+row.split('","')[15]+'",'+'event_time:'+str(fechaevento1)+',custom_data:{phone:"'+textphone+'",email:"'+textemail+'",fn:"'+textfn+'",ln:"'+textln+'",ct:"'+textct+'",st:"'+textst+'",gen:"'+row.split('","')[11]+'",dob:"'+dobfinal+'"}}'

                                # remplazar backslahs
                                # print(textIni)
                                textIni=textIni.replace('\"','\'')

                                dataArray3["data"].append(textIni)
                                dataArrayVista["data"].append(textIni)

                            
                            cont1=cont1+1
                            # print(cont)

                # Descomentar linea 208 para Imprimir información cargada 
                # print(dataArray["data"])
                # Descomentar lineas 210,211 para Imprimir información en un json 
                # with open("test3.json","w")as f:
                #     json.dump(dataArray,f,indent=4)
                # retorna información
                return dataArrayVista

                # realiza inserción en Facebook descomentar desde 702 a la 715
                # if (TipoExtensionArray==1):
                #     self.offline_dataset.create_event(params=dataArray)
                # elif (TipoExtensionArray==2):
                #     self.offline_dataset.create_event(params=dataArray)
                #     self.offline_dataset.create_event(params=dataArray1)
                # elif (TipoExtensionArray==3):
                #     self.offline_dataset.create_event(params=dataArray)
                #     self.offline_dataset.create_event(params=dataArray1)
                #     self.offline_dataset.create_event(params=dataArray2)
                # elif (TipoExtensionArray==4):
                #     self.offline_dataset.create_event(params=dataArray)
                #     self.offline_dataset.create_event(params=dataArray1)
                #     self.offline_dataset.create_event(params=dataArray2)
                #     self.offline_dataset.create_event(params=dataArray3)


a=FacebookService()
vista = a.upload_offline_conversion()
# print(vista)

# app = Flask(__name__)

# @app.route('/')
# def hello():
#     a=FacebookService()
#     vista = a.upload_offline_conversion()
#     return vista

# if __name__ == '__main__':
#     app.run(host='127.0.0.1', port=8080, debug=True)

