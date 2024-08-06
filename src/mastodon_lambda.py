from collections import defaultdict
from datetime import datetime
import faust
import threading
from faust import Schema, Stream
from faust_avro_serializer import FaustAvroSerializer
from schema_registry.client import SchemaRegistryClient
from aux2.faust.models import Toot

from aiohttp.web import Response
from aiohttp_sse import sse_response

#Librerias adicionales
import asyncio
import signal
import time
import readchar
import pandas as pd
import xlsxwriter


#Donde tengo el server -Se lo paso al agente
KAFKA_BOOTSTRAP_SERVER = 'kafka://localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
APP_NAME = 'adsi-' + str(datetime.timestamp(datetime.now()))
FROM_TOPIC = 'mastodon-topic'
MODEL_BATCH_IN_SECONDS = 10.0


# Aqui creo el agente
app = faust.App(APP_NAME, broker=KAFKA_BOOTSTRAP_SERVER)

schema_client = SchemaRegistryClient(SCHEMA_REGISTRY_URL)
serializer = FaustAvroSerializer(schema_client, FROM_TOPIC, False)
schema_with_avro = Schema(key_serializer=str, value_serializer=serializer)

# Son iguales un topic y un channel, excepto en que el channel no tiene nombre
topic = app.topic(FROM_TOPIC, schema=schema_with_avro)
#channel = app.channel()

lock = threading.Lock()
#TODO 2.0: Definir modelos
#Definir el idioma
languages_speed = defaultdict(int)
languages_bots_speed = defaultdict(int)
langugages_batch = defaultdict(int)

# speed layer
#TODO 2.0: Generar capa de velocidad 
      
@app.agent(topic)
async def speed_layer(stream:Stream[Toot]):

    filtrar = False
    # Categoria de filtros

    #Metodo que filtra
    if(filtrar==True):

        async for toot in stream:

            if(filtroCompleto(toot) == True):
                key = toot['language']
                with lock:
                        languages_speed[key] += 1
                #print(languages_speed.items())
                print(toot['language'])
                print(toot)
            else:
                continue            
    else:
        async for toot in stream:
            key = toot['language']
            with lock:
                    languages_speed[key] += 1
            print(languages_speed.items())



# batch layer
#TODO 3.0: Generar capa de batch
#Esta fuuncion se ejecuta cada MODEL_BATCH_IN_SECONDS segundos
@app.timer(MODEL_BATCH_IN_SECONDS)
async def batch_layer():
    
    with lock:
        for key, value in languages_speed.items():
            langugages_batch[key] += value
        # TODO comprobar si hay muchos bots y si no descartar los cambios

        languages_speed.clear()
        print("Modelo batch generado: ", langugages_batch.items(), "----------------------------\n")

# service layer
#TODO 4.0: Generar capa de servicio.

@app.page('/batch-model')
async def batch_model(self, request):
    loop = request.app.loop
    async with sse_response(request) as resp:
        with lock:
            #await resp.send(str(dict(langugages_batch.items())))
            # Ordeno por numero de toots
            sorted_batch = dict(sorted(langugages_batch.items(), key=lambda item:item[1], reverse=True))
            await resp.send(str(sorted_batch))
    return resp

@app.page('/speed-model')
async def speed_model(self, request):
    loop = request.app.loop
    async with sse_response(request) as resp:
        with lock:
            await resp.send(str(dict(languages_speed.items())))
    return resp


# Mejora de la capa de servicio/visualizacion
@app.page('/')
async def index(self, request):
    languages = {
    'aa': 'Afar',
    'ab': 'Abjasio',
    'ae': 'Avéstico',
    'af': 'Afrikáans',
    'ak': 'Akan',
    'am': 'Amárico',
    'an': 'Aragonés',
    'ar': 'Árabe',
    'as': 'Asamés',
    'av': 'Avar',
    'ay': 'Aimara',
    'az': 'Azerí',
    'ba': 'Bashkir',
    'be': 'Bielorruso',
    'bg': 'Búlgaro',
    'bh': 'Bhoyapurí',
    'bi': 'Bislama',
    'bm': 'Bambara',
    'bn': 'Bengalí',
    'bo': 'Tibetano',
    'br': 'Bretón',
    'bs': 'Bosnio',
    'ca': 'Catalán',
    'ce': 'Checheno',
    'ch': 'Chamorro',
    'co': 'Corso',
    'cr': 'Cree',
    'cs': 'Checo',
    'cu': 'Eslavo eclesiástico',
    'cv': 'Chuvasio',
    'cy': 'Galés',
    'da': 'Danés',
    'de': 'Alemán',
    'dv': 'Maldivo',
    'dz': 'Dzongkha',
    'ee': 'Ewe',
    'el': 'Griego',
    'en': 'Inglés',
    'eo': 'Esperanto',
    'es': 'Español',
    'et': 'Estonio',
    'eu': 'Euskera',
    'fa': 'Persa',
    'ff': 'Fula',
    'fi': 'Finés',
    'fj': 'Fiyiano',
    'fo': 'Feroés',
    'fr': 'Francés',
    'fy': 'Frisón',
    'ga': 'Irlandés',
    'gd': 'Gaélico escocés',
    'gl': 'Gallego',
    'gn': 'Guaraní',
    'gu': 'Guyaratí',
    'gv': 'Manés',
    'ha': 'Hausa',
    'he': 'Hebreo',
    'hi': 'Hindi',
    'ho': 'Hiri motu',
    'hr': 'Croata',
    'ht': 'Haitiano',
    'hu': 'Húngaro',
    'hy': 'Armenio',
    'hz': 'Herero',
    'ia': 'Interlingua',
    'id': 'Indonesio',
    'ie': 'Interlingue',
    'ig': 'Igbo',
    'ii': 'Yi de Sichuán',
    'ik': 'Inupiaq',
    'io': 'Ido',
    'is': 'Islandés',
    'it': 'Italiano',
    'iu': 'Inuktitut',
    'ja': 'Japonés',
    'jv': 'Javanés',
    'ka': 'Georgiano',
    'kg': 'Kikongo',
    'ki': 'Kikuyu',
    'kj': 'Kuanyama',
    'kk': 'Kazajo',
    'kl': 'Groenlandés',
    'km': 'Camboyano',
    'kn': 'Canarés',
    'ko': 'Coreano',
    'kr': 'Kanuri',
    'ks': 'Cachemiro',
    'ku': 'Kurdo',
    'kv': 'Komi',
    'kw': 'Córnico',
    'ky': 'Kirguís',
    'la': 'Latín',
    'lb': 'Luxemburgués',
    'lg': 'Luganda',
    'li': 'Limburgués',
    'ln': 'Lingala',
    'lo': 'Lao',
    'lt': 'Lituano',
    'lu': 'Luba-katanga',
    'lv': 'Letón',
    'mg': 'Malgache',
    'mh': 'Marshalés',
    'mi': 'Maorí',
    'mk': 'Macedonio',
    'ml': 'Malayalam',
    'mn': 'Mongol',
    'mr': 'Maratí',
    'ms': 'Malayo',
    'mt': 'Maltés',
    'my': 'Birmano',
    'na': 'Nauruano',
    'nb': 'Noruego bokmål',
    'nd': 'Ndebele del norte',
    'ne': 'Nepalí',
    'ng': 'Ndonga',
    'nl': 'Neerlandés',
    'nn': 'Noruego nynorsk',
    'no': 'Noruego',
    'nr': 'Ndebele del sur',
    'nv': 'Navajo',
    'ny': 'Chichewa',
    'oc': 'Occitano',
    'oj': 'Ojibwa',
    'om': 'Oromo',
    'or': 'Oriya',
    'os': 'Osetio',
    'pa': 'Panyabí',
    'pi': 'Pali',
    'pl': 'Polaco',
    'ps': 'Pastú',
    'pt': 'Portugués',
    'qu': 'Quechua',
    'rm': 'Romanche',
    'rn': 'Kirundi',
    'ro': 'Rumano',
    'ru': 'Ruso',
    'rw': 'Ruandés',
    'sa': 'Sánscrito',
    'sc': 'Sardo',
    'sd': 'Sindhi',
    'se': 'Sami septentrional',
    'sg': 'Sango',
    'si': 'Cingalés',
    'sk': 'Eslovaco',
    'sl': 'Esloveno',
    'sm': 'Samoano',
    'sn': 'Shona',
    'so': 'Somalí',
    'sq': 'Albanés',
    'sr': 'Serbio',
    'ss': 'Suazi',
    'st': 'Sesotho',
    'su': 'Sundanés',
    'sv': 'Sueco',
    'sw': 'Suajili',
    'ta': 'Tamil',
    'te': 'Telugu',
    'tg': 'Tayiko',
    'th': 'Tailandés',
    'ti': 'Tigriña',
    'tk': 'Turcomano',
    'tl': 'Tagalo',
    'tn': 'Setsuana',
    'to': 'Tongano',
    'tr': 'Turco',
    'ts': 'Tsonga',
    'tt': 'Tártaro',
    'tw': 'Twi',
    'ty': 'Tahitiano',
    'ug': 'Uigur',
    'uk': 'Ucraniano',
    'ur': 'Urdu',
    'uz': 'Uzbeko',
    've': 'Venda',
    'vi': 'Vietnamita',
    'vo': 'Volapük',
    'wa': 'Valón',
    'wo': 'Wolof',
    'xh': 'Xhosa',
    'yi': 'Yidis',
    'yo': 'Yoruba',
    'za': 'Zhuang',
    'zh': 'Chino',
    'zu': 'Zulú'
}
    
    # recorrer el diccionario
    t = """
        <html>
        <head>    
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.3.1/dist/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">
        
        <style>
        div {
        margin: 50px;
        }
        </style>
        </head>

        <body >
            <script>
                // setTimeout(() => {
               //     window.location.reload()
               // }, 10000)
                var evtSource = new EventSource("/batch-model");
                evtSource.onmessage = function(e) {
                    //document.getElementById('response').innerText = e.data

                    setTimeout(() => {
                        window.location.reload()
                    }, 10000)
                    //document.getElementById('table-id').innerText = e.data
                    //document.getElementById('table-id')
                }
            </script>
            <h1 class="text-center">Número de toots por idioma:</h1>
            <div id="response"></div>
            <div></div>
            \n
            


<div style="margin: 20px;">




<table class="table" id="table-id">
  <thead>
    <tr>
      <th scope="col">#</th>
      <th scope="col">Siglas</th>
      <th scope="col">Idioma</th>
      <th scope="col">Número de toots</th>
    </tr>
  </thead>
  <tbody>


    """
    sorted_batch = dict(sorted(langugages_batch.items(), key=lambda item:item[1], reverse=True))
    count = 1
    #languages = {'pl': 'polaco', 'en': 'inglés', 'de': 'alemán', 'cs': 'checo', 'fi': 'finlandés', 'es': 'español', 'uk': 'ucraniano', 'nl': 'neerlandés', 'nn': 'noruego nynorsk', 'ro': 'rumano', 'tr': 'turco', 'zh': 'chino', 'ko': 'coreano', 'fr': 'francés', 'ja': 'japonés', 'pt': 'portugués', 'ca': 'catalán', 'ar': 'árabe', 'el': 'griego', 'eu': 'euskera', 'lv': 'letón', 'ru': 'ruso', 'it': 'italiano', 'vi': 'vietnamita', 'nb': 'noruego bokmål', 'gl': 'gallego', 'sk': 'eslovaco', 'id': 'indonesio', 'da': 'danés', 'as': 'asamés', 'sv': 'sueco', 'co': 'corso', 'th': 'tailandés', 'he': 'hebreo', 'lt': 'lituano'}

    print(len(languages.items()))
    for key, value in sorted_batch.items():
        if(value):
            valor = languages[key]


        t += f"<tr><th>{count}</th> <td>{key}</td><td>{valor}</td> <td>{value}</td> </tr>" + "\n"
        count += 1
    
    t += """
      </tbody>
        </table>

        </body>
    </html>
    """
    
    return Response(text=t, content_type='text/html')

# Mejora de la capa batch 
def handler(signum, frame):
    msg = "¿Quieres guardar los datos en un Excel? Y(Si) N(No)"
    print(msg, end="", flush=True)
    res = readchar.readchar()
    if res == 'y':
        print("Guardando datos en XML...")

        #===============================
        copy_dict = langugages_batch.copy()
        sheetName='Toots per language'
        df=pd.DataFrame(copy_dict,[pd.Index(['Nº Toots:'])])
        #df= df.transpose()


        # Creamos el excel y le especificamos el motor
        writer = pd.ExcelWriter("Toot-chart.xlsx", engine="xlsxwriter")
        #Convierto el dataframe a un objeto Excel de XlsxWriter
        df.to_excel(writer, sheet_name=sheetName)
        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        #Aqui le pongo el nombre a la hoja que quiero meterle el grafico
        worksheet = writer.sheets[sheetName]
        # Creamos y especificamos el tipo de grafico
        # Grafico de barras
        chart = workbook.add_chart({"type": "column"})

        # Grafico de tarta
        #chart = workbook.add_chart({"type": "pie"})
        (max_row, max_col) = df.shape
        # Configuramos el grafico
        # Values = (sheetName, first_row, first_col, last_row, last_col)
        chart.add_series({"values": [sheetName, 1, len(copy_dict), max_row, 1],
                          'categories': [sheetName, 0, len(copy_dict), max_row, 1],
})

        chart.set_x_axis({'name': 'Toots por idioma'})
        chart.set_y_axis({'name': 'Valor', 'major_gridlines': {'visible': False}})
        # Donde dibujo el grafico y especifico posicion
        worksheet.insert_chart(3, 3, chart)
        # Cierro el archivo
        writer.close()

        print('Datos guardados en XML')
        #===============================
        exit(1)
    if res == 'n':
        print("Cerrando...")
        exit(1)
    else:
        print("", end="\r", flush=True)
        print(" " * len(msg), end="", flush=True) # clear the printed line
        print("    ", end="\r", flush=True)

signal.signal(signal.SIGINT, handler)

# Mejora de la capa de velocidad
def filtroCompleto(toot):

    res=True
    if (
        toot['bot'] != False or
        #toot['words'] > 5

        # toot['m_id'] != 0 or
        # toot['created_at'] is not None or
        # toot['created_at_str'] != 'unknown' or
        # toot['app'] != 'unknown' or
        # toot['url'] is not None or
        # toot['base_url'] is not None or
        #toot['language'] is not None or
        toot['language'] != 'es'

        # toot['favourites'] != 0 or
        # toot['username'] != '' or
        # toot['tags'] != 0 or
        # toot['characters'] != 0 or
        # toot['mastodon_text'] is not None
    ):
        res=False
    return res


if __name__=='__main__':
    app.main()
