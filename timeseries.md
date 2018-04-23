

```python
import os
os.listdir(os.getcwd())
```




    ['.ipynb_checkpoints',
     'derby.log',
     'timeseries.ipynb',
     'spark-warehouse',
     'metastore_db']




```python
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as f

```


```python
sparkSession = (SparkSession
                .builder
                .appName('example')
                .master('local')
                .enableHiveSupport()
                .getOrCreate())
```


```python
#Check if hive databases are available
sparkSession.sql("""
     show databases
""").toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>databaseName</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>default</td>
    </tr>
    <tr>
      <th>1</th>
      <td>logs</td>
    </tr>
  </tbody>
</table>
</div>




```python
sparkSession.catalog.listDatabases()
```




    [Database(name='default', description='Default Hive database', locationUri='file:/home/jovyan/hivetimeseries/spark-warehouse'),
     Database(name='logs', description='', locationUri='file:/home/jovyan/hivetimeseries/spark-warehouse/logs.db')]




```python
#Check tables in a database
sparkSession.sql("""
     show tables in default
""").toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>database</th>
      <th>tableName</th>
      <th>isTemporary</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>




```python
sparkSession.catalog.listTables("default")

```




    []




```python
sparkSession.sql('create database IF NOT EXISTS logs')
```




    DataFrame[]




```python
sparkSession.catalog.listDatabases()
```




    [Database(name='default', description='Default Hive database', locationUri='file:/home/jovyan/hivetimeseries/spark-warehouse'),
     Database(name='logs', description='', locationUri='file:/home/jovyan/hivetimeseries/spark-warehouse/logs.db')]




```python
# Define schema
schema = StructType([
    StructField("ip", StringType(), True),
    StructField("clientID", StringType(), True),
    StructField("userID", StringType(), True),
    StructField("time", StringType(), True),
    StructField("method", StringType(), True),
    StructField("endpoint", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("response", IntegerType(), True),
    StructField("size", IntegerType(), True)
])
```


```python
geoip_from_csv_df = sparkSession\
 .read.csv("../data/access_logs.csv", schema)
type(geoip_from_csv_df)    
```




    pyspark.sql.dataframe.DataFrame




```python
sparkSession.sql('use logs')
geoip_from_csv_df.write.mode('overwrite').saveAsTable("weblogs")
```


```python
sparkSession.catalog.listTables("logs")
```




    [Table(name='weblogs', database='logs', description=None, tableType='MANAGED', isTemporary=False)]




```python
#query against the hive table
sparkSession.sql("""
select *
from weblogs
where response<>'200'
""").limit(30).toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ip</th>
      <th>clientID</th>
      <th>userID</th>
      <th>time</th>
      <th>method</th>
      <th>endpoint</th>
      <th>protocol</th>
      <th>response</th>
      <th>size</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:05:49 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
    </tr>
    <tr>
      <th>1</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:05:49 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
    </tr>
    <tr>
      <th>2</th>
      <td>68.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:05:49 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
    </tr>
  </tbody>
</table>
</div>




```python
#query against the dataframe
geoip_from_csv_df.select(geoip_from_csv_df.ip,
 geoip_from_csv_df.endpoint)\
 .limit(30).toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ip</th>
      <th>endpoint</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>64.242.88.10</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>64.242.88.10</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
    </tr>
    <tr>
      <th>2</th>
      <td>64.242.88.10</td>
      <td>/mailman/listinfo/hsdivision</td>
    </tr>
    <tr>
      <th>3</th>
      <td>64.242.88.10</td>
      <td>/twiki/bin/view/TWiki/WikiSyntax</td>
    </tr>
    <tr>
      <th>4</th>
      <td>64.240.88.10</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>64.240.88.10</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
    </tr>
    <tr>
      <th>6</th>
      <td>64.240.88.10</td>
      <td>/mailman/listinfo/hsdivision</td>
    </tr>
    <tr>
      <th>7</th>
      <td>64.240.88.10</td>
      <td>/twiki/bin/view/TWiki/WikiSyntax</td>
    </tr>
    <tr>
      <th>8</th>
      <td>68.240.88.10</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>68.240.88.10</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
    </tr>
    <tr>
      <th>10</th>
      <td>68.240.88.10</td>
      <td>/mailman/listinfo/hsdivision</td>
    </tr>
    <tr>
      <th>11</th>
      <td>68.240.88.10</td>
      <td>/twiki/bin/view/TWiki/WikiSyntax</td>
    </tr>
  </tbody>
</table>
</div>




```python
weblogs_df = sparkSession.read.table("weblogs")
weblogs_df.limit(5).toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ip</th>
      <th>clientID</th>
      <th>userID</th>
      <th>time</th>
      <th>method</th>
      <th>endpoint</th>
      <th>protocol</th>
      <th>response</th>
      <th>size</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:05:49 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
    </tr>
    <tr>
      <th>1</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:06:51 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>4523</td>
    </tr>
    <tr>
      <th>2</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:10:02 -0800</td>
      <td>GET</td>
      <td>/mailman/listinfo/hsdivision</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>6291</td>
    </tr>
    <tr>
      <th>3</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:11:58 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/view/TWiki/WikiSyntax</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>7352</td>
    </tr>
    <tr>
      <th>4</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:05:49 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
    </tr>
  </tbody>
</table>
</div>




```python
type(weblogs_df)
```




    pyspark.sql.dataframe.DataFrame




```python
sparkSession.sql("""
   select unix_timestamp() as unix_timestamp
""").toPandas()

```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>unix_timestamp</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1524452023</td>
    </tr>
  </tbody>
</table>
</div>




```python
weblogs_unixtime_df = weblogs_df.withColumn("unixtime", \
                       f.unix_timestamp("time","dd/MMM/yyyy:HH:mm:ss Z"))   
                                       
                          
weblogs_unixtime_df.limit(30).toPandas()    
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ip</th>
      <th>clientID</th>
      <th>userID</th>
      <th>time</th>
      <th>method</th>
      <th>endpoint</th>
      <th>protocol</th>
      <th>response</th>
      <th>size</th>
      <th>unixtime</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:05:49 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
      <td>1078704349</td>
    </tr>
    <tr>
      <th>1</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:06:51 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>4523</td>
      <td>1078704411</td>
    </tr>
    <tr>
      <th>2</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:10:02 -0800</td>
      <td>GET</td>
      <td>/mailman/listinfo/hsdivision</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>6291</td>
      <td>1078704602</td>
    </tr>
    <tr>
      <th>3</th>
      <td>64.242.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:11:58 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/view/TWiki/WikiSyntax</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>7352</td>
      <td>1078704718</td>
    </tr>
    <tr>
      <th>4</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:05:49 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
      <td>1078700749</td>
    </tr>
    <tr>
      <th>5</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:06:51 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>4523</td>
      <td>1078700811</td>
    </tr>
    <tr>
      <th>6</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:20:42 -0800</td>
      <td>GET</td>
      <td>/mailman/listinfo/hsdivision</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>6291</td>
      <td>1078701642</td>
    </tr>
    <tr>
      <th>7</th>
      <td>64.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:15:31:58 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/view/TWiki/WikiSyntax</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>7352</td>
      <td>1078702318</td>
    </tr>
    <tr>
      <th>8</th>
      <td>68.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:16:05:49 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/edit/Main/Double_bounce_sender?topi...</td>
      <td>HTTP/1.1</td>
      <td>401</td>
      <td>12846</td>
      <td>1078704349</td>
    </tr>
    <tr>
      <th>9</th>
      <td>68.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:17:01:56 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1....</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>4523</td>
      <td>1078707716</td>
    </tr>
    <tr>
      <th>10</th>
      <td>68.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:18:10:02 -0800</td>
      <td>GET</td>
      <td>/mailman/listinfo/hsdivision</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>6291</td>
      <td>1078711802</td>
    </tr>
    <tr>
      <th>11</th>
      <td>68.240.88.10</td>
      <td>-</td>
      <td>-</td>
      <td>07/Mar/2004:19:12:58 -0800</td>
      <td>GET</td>
      <td>/twiki/bin/view/TWiki/WikiSyntax</td>
      <td>HTTP/1.1</td>
      <td>200</td>
      <td>7352</td>
      <td>1078715578</td>
    </tr>
  </tbody>
</table>
</div>




```python
weblogs_unixtime_df.groupby("ip")\
         .agg(f.min("unixtime").alias("begin"),
              f.max("unixtime").alias("end"))\
         .limit(30).toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ip</th>
      <th>begin</th>
      <th>end</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>68.240.88.10</td>
      <td>1078704349</td>
      <td>1078715578</td>
    </tr>
    <tr>
      <th>1</th>
      <td>64.240.88.10</td>
      <td>1078700749</td>
      <td>1078702318</td>
    </tr>
    <tr>
      <th>2</th>
      <td>64.242.88.10</td>
      <td>1078704349</td>
      <td>1078704718</td>
    </tr>
  </tbody>
</table>
</div>




```python
weblogs_unixtime_df.groupby("ip")\
         .agg(f.min("unixtime").alias("begin"),
              f.max("unixtime").alias("end"))\
         .select("ip", (f.col("end") - f.col("begin")))\
         .limit(30).toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ip</th>
      <th>(end - begin)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>68.240.88.10</td>
      <td>11229</td>
    </tr>
    <tr>
      <th>1</th>
      <td>64.240.88.10</td>
      <td>1569</td>
    </tr>
    <tr>
      <th>2</th>
      <td>64.242.88.10</td>
      <td>369</td>
    </tr>
  </tbody>
</table>
</div>


