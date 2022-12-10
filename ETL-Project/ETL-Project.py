from pyspark import SparkConf
from pyspark.sql import SparkSession

BUCKET = 
KEY1 = 
KEY2 = 

config = {"spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1",
          "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.f3.s3a.InstanceProfileCredentialsProvider",
}
conf = SparkConf().setAll(confid.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df1 = spark.read.csv(f"s3a://{BUCKET}/{KEY1}", header = True)
df2 = spark.read.csv(f"s3a://{BUCKET}/{KEY2}", header = True)

df1.show()
df2.show()
