from pyspark import SparkContext
import pyspark.sql.types as tp
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, IDF, Tokenizer,StopWordsRemover
import argparse

# Spark configuration
sc = SparkContext(appName = "IMDbTWeet")
spark = SparkSession(sc)
sc.setLogLevel("WARN")



def train(dataset):
    # Training Set

    trainingSchema = tp.StructType([
        tp.StructField(name = 'Sentiment', dataType = tp.IntegerType(), nullable = False), 
        tp.StructField(name = 'SentimentText', dataType = tp.StringType(), nullable = False)
        
    ])


    training_set = spark.read.csv(dataset,
                                schema = trainingSchema,
                                header = True,
                                sep = ',')

    print("Creating ML pipeline...", "\n")

    stage_1 = Tokenizer(inputCol="SentimentText", outputCol="tokens")
    stage_2 = StopWordsRemover(inputCol = 'tokens', outputCol = 'filtered_words')
    stage_3 = HashingTF(numFeatures=2**16, inputCol="filtered_words", outputCol='tf')
    stage_4 = IDF(inputCol='tf', outputCol="vector", minDocFreq=1) 
    lr = LogisticRegression(featuresCol = 'vector', labelCol = 'Sentiment',maxIter=1000)
    pipeline = Pipeline(stages = [stage_1, stage_2, stage_3,stage_4, lr])
    print("Pipeline created", "\n")
    print("Creating logit model...", "\n")
    model = pipeline.fit(training_set)
    print("Logit model created", "\n")

    # Building the model
    modelSummary = model.stages[-1].summary
    print('model accuracy :',  modelSummary.accuracy)

    model.transform(training_set)
    model.save("model")
    print("Logit model saved", "\n")


parser = argparse.ArgumentParser(description="imdb movie training")
parser.add_argument("--dataset", type=str, help="dataset path")
args = parser.parse_args()
train(dataset=args.dataset)


