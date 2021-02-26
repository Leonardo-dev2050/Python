# Python
# Requerimiento toma de los datos historicos desde la API y realiza el almacenamiento en un filesystem y luego la ingesta en el Servidor SQL de samidata.database.windows.net
import http.client
import json
import pyodbc 
import os
import os.path
import glob
import codecs


def eliminarAcentos(cadena):

    d = {    '\xc1':'A',
        '\xc9':'E',
        '\xcd':'I',
        '\xd3':'O',
        '\xda':'U',
        '\xdc':'U',
        '\xd1':'N',
        '\xc7':'C',
        '\xed':'i',
        '\xf3':'o',
        '\xf1':'n',
        '\xe7':'c',
        '\xe1':'a',
        '\xe2':'a',
        '\xe3':'a',
        '\xe4':'a',
        '\xe5':'a',
        '\xe8':'e',
        '\xe9':'e',
        '\xea':'e',
        '\xeb':'e',
        '\xec':'i',
        '\xed':'i',
        '\xee':'i',
        '\xef':'i',
        '\xf2':'o',
        '\xf3':'o',
        '\xf4':'o',
        '\xf5':'o',
        '\xf0':'o',
        '\xf9':'u',
        '\xfa':'u',
        '\xfb':'u',
        '\xfc':'u',
        '\xe5':'a'
}

    nueva_cadena = cadena
    for c in d.keys():
        nueva_cadena = nueva_cadena.replace(c,d[c])

    auxiliar = nueva_cadena.encode('utf-8')
    return nueva_cadena

def writeFiles():

    conn = http.client.HTTPConnection("sanmiguel.cybermapa.com")

    payload = "{\r\n\t\"user\":\"dmazzucco\",\r\n\t\"pwd\":\"Daarma180!\",\r\n\t\"action\": \"viajes_sanmiguel\"}\r\n"

    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        'postman-token': "1c93d3fd-8dbc-feae-911b-13340f267c13"
        }

    conn.request("POST", "/API/WService.js", payload, headers)

    res = conn.getresponse()
    data = res.read()

    resultado = json.loads(data.decode("latin-1"))
    for token in resultado:
        f = open("c:\\tmp\\v" + token["id_de_viaje"] + ".json", "w+")
        datos = eliminarAcentos(str(token).replace("' '", 'null').replace("'", '"'))
        f.write(datos)
        f.close()

def write_table(rows):
        server = 'samidata.database.windows.net'
        username = 'samidatauser'
        password = "mSxQUm2)'kX*8qki"
        driver = '{SQL Server}'
        database = 'samidm'
        sql1 = "declare @json nvarchar(max) = '" + rows + "'"
        sql2 = "INSERT INTO dbo.cbmp_ft_viajes select * from OPENJSON(@json) WITH (id_de_viaje varchar(10), tipo_de_origen varchar(10),   origen varchar(125),     tipo_de_destino varchar(10),     destino  varchar(125),     str_origen varchar(125),     str_destino varchar(125),     carga varchar(10) ,     nro_pedido varchar(10),     fecha_programada_origen datetime ,    fecha_programada_destino  datetime ,    fecha_maxima_asignacion datetime , Estado varchar(40),      fecha_estimada_origen datetime ,    fecha_ingreso_origen datetime ,     fecha_egreso_origen datetime ,     entrega  varchar(10) ,     fecha_de_cancelacion datetime ,     interno varchar(40), tipo varchar(40), texto_de_cancelacion  varchar(125) ,    Nro_Remito  varchar(10) ,     Cantidad  varchar(10) ,     Pagar  varchar(10)  )"

        connection = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
        with connection:
            cursor =  connection.cursor()
            cursor.execute(sql1 + sql2)


def deleteTable():
        server = 'samidata.database.windows.net'
        username = 'samidatauser'
        password = "mSxQUm2)'kX*8qki"
        driver = '{SQL Server}'
        database = 'samidm'
        sql = "truncate table dbo.cbmp_ft_viajes"

        connection = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
        with connection:
            cursor =  connection.cursor()
            cursor.execute(sql)

def writeRows():
    dir = "c:\\tmpSemanal"
    os.chdir(dir)

    for viaje in glob.glob("v*.json"):
         f=codecs.open(viaje, encoding="latin-1")
         datos = f.read()
         write_table(datos)
         f.close()



if __name__ == '__main__':
	writeFiles()
	#deleteTable()
	writeRows()
