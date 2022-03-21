# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run "/BigDataLab/download_data"

# COMMAND ----------

# MAGIC %md Names.csv 
# MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
# MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
# MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
# MAGIC * Dodaj kolumnę i policz wiek aktorów 
# MAGIC * Usuń kolumny (bio, death_details)
# MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
# MAGIC * Posortuj dataframe po imieniu rosnąco

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Files/names.csv"
namesDf = spark.read.format("csv") \
  .option("header","true") \
  .option("inferSchema","true") \
  .load(filePath)

display(namesDf)

# COMMAND ----------

namesDf.printSchema()

# COMMAND ----------

names1 = namesDf.withColumn("timestamp", F.unix_timestamp())
# display(names1)

# COMMAND ----------

names2 = namesDf.withColumn("height (ft)", F.round(namesDf.height / 30.480, 2))
# display(names2)

# COMMAND ----------

names3 = namesDf \
  .select(F.split('name', ' ')[0].alias('first_name')) \
  .groupBy('first_name') \
  .count() \
  .orderBy(F.desc('count')) \
  .limit(1)

display(names3)

# COMMAND ----------

names4 = namesDf.withColumn('age', F.year(namesDf.date_of_death) - F.year(namesDf.date_of_birth))
# display(names4) # w tym przypadku wiekszosc zwraca null poniewaz daty sa w roznych formatach

# COMMAND ----------

names5 = namesDf.drop('bio', 'death_details')
# display(names5)

# COMMAND ----------

colnames = [''.join(x.capitalize() or '_' for x in word.split('_')) for word in namesDf.columns]
names6 = namesDf.toDF(*colnames)
# display(names6)

# COMMAND ----------

names7 = namesDf \
  .withColumn('first_name', F.split('name', ' ')[0]) \
  .orderBy('first_name')
# display(names7)

# COMMAND ----------

# MAGIC %md Movies.csv
# MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
# MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
# MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
# MAGIC * Usuń wiersze z dataframe gdzie wartości są null

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Files/movies.csv"
moviesDf = spark.read.format("csv") \
  .option("header","true") \
  .option("inferSchema","true") \
  .load(filePath)

display(moviesDf)

# COMMAND ----------

movies1 = moviesDf.withColumn("timestamp", F.unix_timestamp())
# display(movies1)

# COMMAND ----------

movies2 = moviesDf.withColumn("aged", F.year(F.current_date()) - moviesDf.year)
# display(movies2)

# COMMAND ----------

movies3 = moviesDf.withColumn("budget_num", F.regexp_extract(moviesDf.budget, '(\d+)', 1))
# display(movies3)

# COMMAND ----------

movies4 = moviesDf.dropna()
# display(movies4)

# COMMAND ----------

# MAGIC %md ratings.csv
# MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
# MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
# MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
# MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
# MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
# MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
ratingsDf = spark.read.format("csv") \
  .option("header","true") \
  .option("inferSchema","true") \
  .load(filePath)

display(ratingsDf)

# COMMAND ----------

ratings1 = ratingsDf.withColumn("timestamp", F.unix_timestamp())
# display(ratings1)

# COMMAND ----------

# poprawic

from numpy import median

def row_median(columns):
  return median[int(x) for x in columns]

udf_median = udf(my_median, IntegerType())

colnames = ratingsDf.columns[5:15]
ratings2 = ratingsDf \
  .withColumn("mean", sum([F.col(x) for x in colnames])/len(colnames))
#   .withColumn("median", F.lit([F.col(x) for x in colnames]))
# display(ratings2)

# COMMAND ----------

ratings3 = ratingsDf \
  .withColumn("weighted - mean", F.abs(ratingsDf.weighted_average_vote - ratingsDf.mean_vote)) \
  .withColumn("weighted - median", F.abs(ratingsDf.weighted_average_vote - ratingsDf.median_vote))
  
# display(ratings3)

# COMMAND ----------

ratings4 = ratingsDf \
  .select("females_allages_avg_vote","males_allages_avg_vote") \
  .agg(F.avg("females_allages_avg_vote"), F.avg("males_allages_avg_vote"))
display(ratings4)
# Kobiety oceniaja lepiej srednio o 0.3 punktu

# COMMAND ----------

from pyspark.sql.types import DecimalType

ratings5 = ratingsDf.withColumn("mean_vote",ratingsDf.total_votes.cast(LongType()))
# ratings5.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Zadanie 2 - Spark UI
# MAGIC 
# MAGIC **Jobs** - informacje o statusie i czasie trwania jobów, zawiera timeline i wizualizację DAG \
# MAGIC **Stages** - stan wszystkich etapów zadania, w przypadku nieudanych jest pokazana przyczyna \
# MAGIC **Storage** - wyświetla partycje i ich wielkość, pokazuje RDDs i DataFrames \
# MAGIC **Environment** - zawiera informacje o zmiennych środowiskowych i ustawienia systemowe, można tu szukac informacji np. o JVM, wersjach Javy czy Scali, właściwościach Sparka \
# MAGIC **Executors** - pokazuje informacje o executorach jak wykorzystanie pamięci i dysku, wykonywanych zadaniach \
# MAGIC **SQL** - lista zapytań SQL, ich fizyczne i logiczne plany wykonania

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Zadanie 3 - groupBy

# COMMAND ----------

movies_grouped = moviesDf.groupBy('year').agg({'duration': 'mean', 'votes': 'max'})
moviesDf.explain()
movies_grouped.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Zadanie 4 - SQL Server

# COMMAND ----------

jdbcDF = spark.read \
  .format("jdbc") \
  .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb") \
  .option("user", "sqladmin") \
  .option("password", "$3bFHs56&o123$") \
  .option("dbtable", "(SELECT * from INFORMATION_SCHEMA.TABLES) emp_alias") \
  .load()

display(jdbcDF)
