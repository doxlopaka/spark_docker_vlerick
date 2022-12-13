from pyspark import SparkConf
from pyspark.sql import SparkSession



import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Lasso, Ridge
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor

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

df = df1.merge(df2, on='movie_title', how='inner')

#IMDB Code

