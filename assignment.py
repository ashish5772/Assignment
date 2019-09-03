from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.functions  as F
df=spark.read.option('header','true').csv(r'path/data.csv') #gives a dataframe
df.write.partitionBy('Country').parquet(r'path/df_prq.parquet')
pq_df=spark.read.parquet(r'path/df_prq.parquet')
split=F.split(df['Values',';']))
for i in range(5):
    df=df.withColumn('V{}'.format(str(i)),split.getItem(i))
df2=df.groupBy(df['Country']).agg(sum('V0').alias('a'),sum('V1').alias('b'),sum('V2').alias('c'),sum('V3').alias('d'),sum('V4').alias('e'))
df2.createOrReplaceTempView(tbl)
finl_df=spark.sql("select Country, concat(a,';',b,';',c,';',d,';',e) from tbl")
finl_df.write.parquet(r'path/ouput.parquet')
#somenewcomment

