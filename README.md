# Lab01

Run HadoopJob

```bash
hadoop jar NewsJobs-1.0-SNAPSHOT.jar uniandes.lab01.hadoop.NewsHadoopJob /datos/reuters/ /user/bigdata7/reto-hadoop-out
```

Run Spark

```bash
spark-submit --class uniandes.lab01.spark.NewsSparkJob --master yarn-client NewsJobs-1.0-SNAPSHOT.jar /datos/reuters/ /user/bigdata7/reto-spark-out
```

Sample in results in Hadoop
 CANADIAN OIL COMPANIES RAISE CRUDE PRICES  165

Sample in results in Spark
( CANADIAN OIL COMPANIES RAISE CRUDE PRICES,165)
