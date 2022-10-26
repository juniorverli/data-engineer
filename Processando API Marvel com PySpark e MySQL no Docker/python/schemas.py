from pyspark.sql.types import *

comicsSchema = StructType(
    [
        StructField('id', IntegerType(), True),
        StructField('digitalId', IntegerType(), True),
        StructField('title', StringType(), True),
        StructField('issueNumber', IntegerType(), True),
        StructField('variantDescription', StringType(), True),
        StructField('description', StringType(), True),
        StructField('modified', StringType(), True),
        StructField('isbn', StringType(), True),
        StructField('upc', StringType(), True),
        StructField('diamondCode', StringType(), True),
        StructField('ean', StringType(), True),
        StructField('issn', StringType(), True),
        StructField('format', StringType(), True),
        StructField('pageCount', StringType(), True),
        StructField('resourceURI', StringType(), True),
        StructField('thumbnail', StructType([
            StructField("path", StringType(), True)
        ]), True),
        StructField('prices', ArrayType(StructType([StructField('price', StringType(), True)])), True)
    ]
)

charactersSchema = StructType(
    [
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True),
        StructField('description', StringType(), True),
        StructField('modified', StringType(), True),
        StructField('resourceURI', StringType(), True),
        StructField('thumbnail', StructType([
            StructField("path", StringType(), True)
        ]), True),
        StructField('comics', StructType([
            StructField("items", ArrayType(StructType([StructField('resourceURI', StringType(), True)])),True)
        ]),True)
    ]
)
