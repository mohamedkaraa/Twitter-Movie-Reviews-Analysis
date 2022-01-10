import json
from pyspark import SparkContext
import pyspark.sql.types as tp
from pyspark.sql.session import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import when
from elasticsearch import Elasticsearch

# Spark
sc = SparkContext(appName = "IMDbTWeet")
spark = SparkSession(sc)
sc.setLogLevel("WARN")
# Kafka
kafkaServer = "localhost:9092"
brokers = "localhost:9092"
topic = "tweets"
# Elasticsearch
elastic_host = "localhost"
elastic_index = "twitter"
elastic_document = "_doc"




# spark dataframe
twitterSchema = tp.StructType([
    tp.StructField("Title", tp.StringType(), True),
    tp.StructField("SentimentText", tp.StringType(),True)
])
tweetStorage = spark.createDataFrame(sc.emptyRDD(), twitterSchema)


# elasticsearch mapping
es_mapping = {
        "properties":
        {
            "Title": {"type": "constant_keyword", "fielddata": True},
            "SentimentText": {"type": "text", "fielddata": True},
            "Sentiment": {"type": "text"}
        }
    }

es = Elasticsearch(hosts = elastic_host)


response = es.indices.create(
    index = elastic_index,
    mappings = es_mapping,
    ignore = 400 # Ignore Error 400 Index already exists
)



if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print("Index mapping SUCCESS: ", response['index'])
        

def mapinfo(batchDF, batchID):
    print("******** foreach ********", batchID)
    valueRdd = batchDF.rdd.map(lambda x: x[1])
    print(valueRdd, "\n")
    strList = valueRdd.map(lambda x: json.loads(x)).collect() #list of strings with json value
    print("\n", strList, "\n")
    tupleList = []
    for string in strList:
        splitStr = string.split('","')
        titleStr = splitStr[0].split('":"', 1)[1]
        tweetStr = splitStr[1].split('":"', 1)[1]
        tuple = (titleStr, tweetStr[:len(tweetStr) - 2])
        tupleList.append(tuple)
        tweetDF = spark.createDataFrame(data = tupleList, schema = twitterSchema)
        loaded_model = PipelineModel.load("model")
        transformedDF = loaded_model.transform(tweetDF).select('Title', 'SentimentText', 'prediction')
        transformedDF.show(truncate = False)
        transformedDF = transformedDF.withColumn('prediction',when(transformedDF.prediction == 0,'negative').otherwise('positive'))
        if (transformedDF.select("SentimentText").collect()[0].asDict()["SentimentText"].lower() not in transformedDF.select("Title").collect()[0].asDict()["Title"].lower() ):

            transformedDF.write \
            .format("org.elasticsearch.spark.sql") \
            .mode("append") \
            .option("es.nodes", elastic_host).save(elastic_index)
            
    
    
print("Reading from topic")
kafkaDataFrame = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .load()


df2 = kafkaDataFrame \
    .writeStream \
    .foreachBatch(mapinfo) \
    .start() \
    .awaitTermination()

