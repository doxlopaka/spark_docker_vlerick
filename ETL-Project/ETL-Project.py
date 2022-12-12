from pyspark import SparkConf
from pyspark.sql import SparkSession

BUCKET = "dmacademy-course-assets" 
KEY1 = "vlerick/after_release.csv"
KEY2 = "vlerick/pre_release.csv"

config = {"spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1",
          "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.f3.s3a.InstanceProfileCredentialsProvider",
}
conf = SparkConf().setAll(confid.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df1 = spark.read.csv(f"s3a://{BUCKET}/{KEY1}", header = True)
df2 = spark.read.csv(f"s3a://{BUCKET}/{KEY2}", header = True)

df1.show()
df2.show()
