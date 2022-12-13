from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor

BUCKET = "dmacademy-course-assets" 
KEY1 = "vlerick/after_release.csv"
KEY2 = "vlerick/pre_release.csv"

config = {"spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1"
}

'''
config = {"spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1",
          "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.f3.s3a.SimpleAWSCredentialsProvider"
}
'''


conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df1 = spark.read.csv(f"s3a://{BUCKET}/{KEY1}", header = True).toPandas()
df2 = spark.read.csv(f"s3a://{BUCKET}/{KEY2}", header = True).toPandas()

df1.head()
df2.head()

df_raw = df1.merge(df2, on='movie_title', how='inner')

#IMDB Code
df_raw.drop_duplicates(inplace = True)


#Director and actor names are not useful seeing as 
#I already have their facebook likes which would carry 
# information about their popularity.
df_raw = df_raw.drop(['director_name', 'actor_1_name', 'actor_2_name', 'actor_3_name', 'movie_title'], axis = 'columns')
df_raw = df_raw.dropna()

#Seeing as the most common countries are the USA, the UK, France and Canada I want to 
#lump all other countries under Other Country.
#Create a new list of all countries 
country_lst = df_raw['country'].unique().tolist()

# Remove the languages that I actually want to keep so I can delete the others
country_lst.remove('USA')
country_lst.remove('UK')
country_lst.remove('France')
country_lst.remove('Canada')

#Loop over lst to change country to "Other Country"
for country in country_lst:
    df_raw = df_raw.replace(country, 'Other Country')

#Seeing as the most common language is English, I want to lump 
#all other languages under Other Language.
#Create a new list of all languages 
language_lst = df_raw['language'].unique().tolist()

# Remove the languages that I actually want to keep so 
# I can delete the others
language_lst.remove('English')
print(language_lst)
#Loop over lst to change languages to "Other Language"
for lang in language_lst:
    df_raw = df_raw.replace(lang, 'Other Language')

#PG, GP and PG-13 are the same
#Not Rated and Unrated are the same
df_raw=df_raw.replace('GP', 'PG')
df_raw=df_raw.replace('PG-13', 'PG')
df_raw=df_raw.replace('Not Rated', 'Unrated')

#Splitting and dummyfying genres
df_raw = pd.concat([df_raw, df_raw['genres'].str.get_dummies('|')], axis=1)
df_raw.drop('genres', axis=1, inplace=True)
df_raw

#Dummyfying content rating
df_raw = pd.concat([df_raw, df_raw['content_rating'].str.get_dummies()], axis=1)
df_raw.drop('content_rating', axis=1, inplace=True)
df_raw

#Dummyfying language
df_raw = pd.concat([df_raw, df_raw['language'].str.get_dummies()], axis=1)
df_raw.drop('language', axis=1, inplace=True)
df_raw

#Dummyfying Country
df_raw = pd.concat([df_raw, df_raw['country'].str.get_dummies()], axis=1)
df_raw.drop('country', axis=1, inplace=True)
df_raw

df = df_raw
print(df.shape)

predictors = df[['director_facebook_likes', 'actor_3_facebook_likes', 'actor_1_facebook_likes', 
    'cast_total_facebook_likes', 'budget', 'actor_2_facebook_likes', 'num_critic_for_reviews',
    'movie_facebook_likes']]

X_train, X_test, y_train, y_test = train_test_split(predictors, df['gross'], test_size=0.25)
print(X_train.shape)
print(X_test.shape)

#Random Forest Regressor Regressor
rfreg = RandomForestRegressor(max_depth=10, min_samples_leaf =1, random_state=0).fit(X_train, y_train)

#predict regression forest
array_pred = np.round(rfreg.predict(X_test),0)

y_pred = pd.DataFrame({"y_pred": array_pred},index=X_test.index) #index must be same as original database
val_pred = pd.concat([y_test,y_pred,X_test],axis=1)

def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return DoubleType()
    elif f == 'float32': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)


def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return spark.createDataFrame(pandas_df, p_schema)

val_pred = pandas_to_spark(val_pred)
val_pred.write.json(f"s3a://{BUCKET}/vlerick/ahmad_flaifel")