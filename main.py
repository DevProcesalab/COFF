import csv
import json
import time
import pandas as pd
from ftplib import FTP
from io import BytesIO
from datetime import date, timedelta, datetime
from hashlib import sha256
from facebook_business.adobjects.serverside.custom_data import CustomData
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.user_data import UserData
from facebook_business.adobjects.serverside.gender import Gender
from facebook_business.api import FacebookAdsApi

#Función para cambiar letras con acentuación
# def normalize(s):
#         replacements = (
#             ("á", "a"),
#             ("é", "e"),
#             ("í", "i"),
#             ("ó", "o"),
#             ("ú", "u"),
#             ("á", "a"),
#             ("é", "e"),
#             ("í", "i"),
#             ("ó", "o"),
#             ("ú", "u"),
#             ("ñ", "n"),
#             ("Á", "A"),
#             ("É", "E"),
#             ("Í", "I"),
#             ("Ó", "O"),
#             ("Ú", "U"),
#             ("Ñ", "N"),
#             ("á", "a"),
#             ("é", "e"),
#             ("í", "i"),
#             ("ó", "o"),
#             ("ú", "u"),
#             ("ñ", "n"),
#             ("Á", "A"),
#             ("É", "E"),
#             ("Í", "I"),
#             ("Ó", "O"),
#             ("Ú", "U"),
#             ("Ñ", "N"),
#         )
#         for a, b in replacements:
#             s = s.replace(a, b).replace(a.upper(), b.upper())
#         return s


def getNameCSV(pixel_id, access_token): #Función para traer nombre del archivo csv
    # Inicializar variable y arreglos
    lines=[]
    listTemp = []
    cuenta_temp = ''
    # Inicializar arreglo principal
    print("inicializacion de librerias")
    # Inicializar variables de conexion al servidor ftp Crystal
    host = 'intfs.crystal.com.co'
    user  = 'ftpadbid'
    password = 'Crystal2021.*'
    print('intentando conexión')
    #Se intenta establecer conexión
    try:
        # se realiza la conexión al ftp del cliente
        ftp = FTP(host)
        ftp.login(user,password)
        print('Conexion establecida')

    except Exception as e:
        print('Conexión Errada: '+str(e))

    #Ingresamos a la carpeta en donde se ubican los csv dentro del servidor
    ftp.cwd('Mercadeo_Ventasoff')
    print('Ahora estamos en la carpeta'+ftp.pwd()+'\n')

    # Agregar listado de archivos ubicados en servidor a lines[]
    ftp.dir(ftp.pwd(),lines.append)

    # Inicializar variables fechas de proceso
    yesterday = date.today() - timedelta(days=1)
    yesterday = yesterday.strftime('%m-%d-%y')
    fecha_hoy = pd.to_datetime('today').strftime('%m-%d-%y')
    # print(yesterday)
    # print(fecha_hoy)
    print(ftp.pwd())

    for line in lines :

        tokens = line.split(maxsplit = 9)
        listTemp = tokens[3].split('_')
        cuenta_temp = listTemp.pop()

        if((tokens[0]==yesterday) & (cuenta_temp=='GLX.csv')):
            
            route = '/Mercadeo_Ventasoff/'+tokens[3]
            r = BytesIO()
            ftp.retrbinary('RETR /Mercadeo_Ventasoff/'+tokens[3], r.write)
            data = str(r.getvalue().decode('utf8'))
            datatemp = data.split('\n')
            length_headerCsv = len(datatemp[0].split('","'))

            main(pixel_id, access_token,datatemp, tokens[3])
            

def main(pixel_id, access_token, dataCsv, nameCsv):

    FacebookAdsApi.init(access_token=access_token)
    
    # Se lee el archivo, desde la segunda fila; teniendo en cuenta que la primer fila es el encabezado del csv. (Para eso es la variable counter!=0 que se ve dentro de los if elif)
    # Se toma la fecha actual vs la fecha del evento.
    # Se resta la fecha actual - la fecha del evento. Esta arroja la diferencia entre días. Si es mayor a 7 o está entre 0 y 7 días, subirá la conversión.
    # Arroja la cantidad de filas o registros que no subió en caso que no esté dentro del rango de 7 días.
    # Se hashean cada uno de los campos que pide la API de Facebook.
    
    yesterday = date.today() - timedelta(days=1)
    counter = 0
    counter_not_upload = 0
    time_today = datetime.today().strftime('%d/%m/%Y')
    time_today_timestamp = time.mktime(datetime.strptime(time_today, '%d/%m/%Y').timetuple())
    print("dentro de with")
    for row in dataCsv:
        if counter != 0:
            checkout = row.split('","')[13]
            checkout_timestamp = time.mktime(datetime.strptime(checkout, '%d/%m/%Y').timetuple())
            diff_days = round(int(float(time_today_timestamp)-float(checkout_timestamp))/(60*60*24))
            email = sha256(row.split('","')[0].encode('utf-8')).hexdigest()
            phone = sha256(row.split('","')[1].encode('utf-8')).hexdigest()
            madid = row.split('","')[2]
            fn = sha256(row.split('","')[3].encode('utf-8')).hexdigest()
            ln = sha256(row.split('","')[4].encode('utf-8')).hexdigest()
            zip = sha256(row.split('","')[5].encode('utf-8')).hexdigest()
            ct = sha256(row.split('","')[6].encode('utf-8')).hexdigest()
            st = sha256(row.split('","')[7].encode('utf-8')).hexdigest()
            country = sha256(row.split('","')[8].encode('utf-8')).hexdigest()
            dob = sha256(row.split('","')[9].encode('utf-8')).hexdigest()
            doby = sha256(row.split('","')[10].encode('utf-8')).hexdigest()
            gen = (row.split('","')[11])
            if(gen=="F"):
                gen = "f"
            elif(gen == "M"):
                gen = "m"

            # texts = [[words for words in sentences.lower().split()] for sentences in gen]
            gender = Gender(gen)
            print(gender)
            print(type(gender))
            # gen = sha256(row.split('","')[11].encode('utf-8')).hexdigest()
            age = sha256(row.split('","')[12].encode('utf-8')).hexdigest()
            event_time = row.split('","')[13]
            value = row.split('","')[14].replace('$', '').replace(',','.').replace('€', '')
            event_name = row.split('","')[15]
            currency = row.split('","')[16]
            tempDateString = row.split('","')[13]
            
            if diff_days > 0 and diff_days <= 7:

                user_data = UserData(
                    email= email,
                    phone= phone,
                    gender= Gender(gen),
                    first_name= fn,    
                    country_code= country,
                    zip_code= zip,                
                )

                custom_data = CustomData(
                    value=float(value),
                    currency=currency,
                    #custom_properties=madid,
                )

                event = Event(
                    event_name=event_name,
                    event_time=int(checkout_timestamp),
                    user_data=user_data,
                    custom_data=custom_data,
                )
                
                events = [event]

                # print((events)) 
                # print(user_data)
                # print(gender)
                # print(type(gender))
                
                event_request = EventRequest(
                    pixel_id=pixel_id,
                    events=events,
                    upload_tag = "base.csv",
                )
                print((event_request)) 

                try:
                    event_response = event_request.execute()
                except (Exception) as error:
                    pass
                    # print('Algo ha ocurrido: '+ error)
                    

                # print(event_response)
                print('OK')
            else:
                counter_not_upload+=1
    
        counter += 1
    print('La cantidad de registros no subidos fue de: ', counter_not_upload)
    

if __name__ == '__main__':
    print("Estamos dentro del if")
    # Se declaran las variables para acceso a la API e ID del pixel asociado a la cuenta BM.
    access_token = 'EAAGFZCXGZByIUBANZCBM9eZBH5ikGwmRgj7fZC7jY0ztc1agSBuogjYqHNokGZBfABF7mr6yHBTwjJa91ZA25rBhL4xUiqq69TqPjj7W33vypOgZBViy4nBt22A1RZBAhFEKsZCW5Nw6ZArUZAKcPYgJW5a5b4G2j1XwliWSuTvjGOXT5gZDZD'
    # pixel_id = '428798558259333'
    pixel_id = '624148101827847' 

    # Valida que exista o esté inicializado el pixel id y el access token
    if not (pixel_id and access_token):
        raise Exception('Validar que esté definido correctamente el pixel id: {pixel_id} y el access token: {access_token}.'.format(pixel_id, access_token)) 

    getNameCSV(pixel_id, access_token)