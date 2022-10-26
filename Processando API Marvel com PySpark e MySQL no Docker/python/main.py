from time import sleep
from pyspark.sql.session import SparkSession
import hashlib
import requests
import json
import pyspark.sql.functions as F
from schemas import *
import mysql.connector

#Colocar as chaves da API cadastradas no site https://developer.marvel.com/
PRIVATE_KEY = ''
PUBLIC_KEY = ''
TIMESTAMP = '10'

COMBINED_KEYS = TIMESTAMP+ PRIVATE_KEY + PUBLIC_KEY
MD5HASH = hashlib.md5(COMBINED_KEYS.encode()).hexdigest()

#Configurações de conexão com o MySQL
CONECTOR_TYPE = "com.mysql.cj.jdbc.Driver"
MYSQL_USERNAME = "root"
MYSQL_PASSWORD = "root"
MYSQL_DBNAME = "db"
MYSQL_SERVERNAME = "db_MYSQL"
MYSQL_PORT = "3306"
url_mysql = f'jdbc:mysql://{MYSQL_SERVERNAME}:{MYSQL_PORT}/{MYSQL_DBNAME}'

def get_api_marvel(session, endpoint, offset):

    sleep(2)
    url = f'https://gateway.marvel.com:443/v1/public/{endpoint}?ts={TIMESTAMP}&apikey={PUBLIC_KEY}&hash={MD5HASH}&offset={offset}&limit=100'
    try:
        r = session.get(url)
        r_json = r.json()
        error = False
    except ConnectionError as e:
        print("CONNECTION ERROR: ")
        print(e)
        r_json = ''
        error = True

    return r_json, error
    
#Salva o arquivo para depois ser processado via PySpark
def write_json(endpoint, get_endpoint):
    with open(f'/usr/app/src/files/{endpoint}_data.json', 'w') as outfile:
        json.dump(get_endpoint['data']['results'], outfile)

#Lê o arquivo via PySpark de acordo com o schemas em schemas.py
def read_json_with_spark(endpoint, schema):
    result = spark.read.schema(schema).json(f'/usr/app/src/files/{endpoint}_data.json', multiLine = "true")
    return result

#Salva os dados no Banco MySQL
def write_mysql(endpoint, rawDF, mode):
    rawDF.write \
        .format("jdbc") \
        .option("url", url_mysql) \
        .option("driver", CONECTOR_TYPE) \
        .option("dbtable", endpoint) \
        .option("user", MYSQL_USERNAME) \
        .option("password", MYSQL_PASSWORD) \
        .mode(mode) \
        .save()

#Lê os dados do banco
def read_with_spark(endpoint):
    result = spark.read \
        .format("jdbc") \
        .option("url", url_mysql) \
        .option("driver", CONECTOR_TYPE) \
        .option("dbtable", endpoint) \
        .option("user", MYSQL_USERNAME) \
        .option("password", MYSQL_PASSWORD) \
        .load()
    return result

#Processo de extração
def extraction(endpoint, schema):

    #Aqui é buscado o último offset executado na API para que caso o código caia em algum momento ele poderá ser executado da onde parou
    sql_config = f"SELECT * FROM config WHERE name = '{endpoint}'"
    cursor.execute(sql_config)
    offset = cursor.fetchone()
    offset = int(offset[1])

    s = requests.Session()

    #Faz a consulta dos dados a partir do ultimo offset registrado, retorna o total de dados e confirma se não houve algum erro durante a conexão
    get_endpoint = get_api_marvel(s, endpoint, offset)
    while get_endpoint[1] is True:
         get_endpoint = get_api_marvel(s, endpoint, offset)

    total = get_endpoint[0]['data']['total']

    #Enquanto o valor de offset for menor que o valor total ele realiza a consulta na base da Marvel
    while offset < total:

        write_json(endpoint, get_endpoint[0])
        rawDF = read_json_with_spark(endpoint, schema)

        #Aqui é realizado o parse dentro do JSON e renomeado as colunas quando necessário.
        if endpoint == 'comics':

            rawDF = rawDF.withColumn("thumbnail", F.col("thumbnail.path"))\
                .withColumn("prices", F.explode_outer("prices"))\
                .withColumn("prices", F.col("prices.price"))\
                .withColumnRenamed("prices", "price")\
                .withColumn("modified", F.to_date("modified"))\
                .filter(rawDF.title.isNotNull())

        elif endpoint == 'characters':

            rawDF = rawDF.withColumn("comics", F.arrays_zip("comics.items"))\
                .withColumn("comics", F.explode_outer("comics.items"))\
                .withColumn("comics", F.col("comics.resourceURI"))\
                .withColumn("comics", F.expr("substring(comics, 44, length(comics)-43)"))\
                .withColumnRenamed("comics", "comicId")\
                .withColumn("thumbnail", F.col("thumbnail.path"))\
                .withColumn("modified", F.to_date("modified"))
    
        #Cria a tabela de cada endpoint e caso já esteja criada ela inclui os dados consultados anteriormente
        write_mysql(endpoint, rawDF, "append")

        #Atualiza a quantidade de offset no banco/código
        offset += 100
        update_offset = f"UPDATE config SET offset = {offset} WHERE name = '{endpoint}'"
        cursor.execute(update_offset)
        cnx.commit()

        get_endpoint = get_api_marvel(s, endpoint, offset)
        while get_endpoint[1] is True:
            get_endpoint = get_api_marvel(s, endpoint, offset)

def main():

    extraction('comics', comicsSchema)
    extraction('characters', charactersSchema)

    #Renomeia todas as colunas de comics e characters para que não haja conflito durante a junção das duas tabelas
    comics = read_with_spark('comics')
    for column in comics.columns:
        comics = comics.withColumnRenamed(column, "comics" + "_" + column)

    characters = read_with_spark('characters')
    for column in characters.columns:
        characters = characters.withColumnRenamed(column, "characters" + "_" + column)

    #Realiza a criação de uma OBT em um Join entre as tabelas comics e characters
    obtDF = characters.join(comics, characters.characters_comicId == comics.comics_id, how = 'inner') \
        .drop("characters_comicId") \
        .dropDuplicates()

    #Salva no banco os dados da OBT anterior sempre reescrevendo os dados.
    write_mysql('all_comics_and_characters', obtDF, "overwrite")

if __name__ == '__main__':

    #Configurações iniciais do PySpark Local utilizando o driver do MySQL para se conectar
    spark = SparkSession.builder \
            .appName("PySpark") \
            .master("local[*]") \
            .config("spark.driver.extraClassPath", "/usr/app/src/java/mysql-connector-java-8.0.22.jar") \
            .getOrCreate()

    #Aqui se conecta e criar a tabela CONFIG para realizar as validações do ultimo offset por endpoint
    cnx = mysql.connector.connect(
    host=MYSQL_SERVERNAME, user=MYSQL_USERNAME, password=MYSQL_PASSWORD, database=MYSQL_DBNAME, port=MYSQL_PORT)
    cursor = cnx.cursor()

    sql = "CREATE TABLE IF NOT EXISTS config \
        SELECT name, offset FROM (VALUES ROW('comics', 0), \
        ROW('characters', 0) ) As config (name, offset)"
    
    cursor.execute(sql)

    main()