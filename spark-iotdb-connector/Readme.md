## version

The versions required for Spark and Java are as follow:

| Spark Version | Scala Version | Java Version | TsFile |
| ------------- | ------------- | ------------ |------------ |
| `2.4.3`        | `2.11`        | `1.8`        | `0.9.0-SNAPSHOT`|


## install
mvn clean scala:compile compile install


## maven dependency

```
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>spark-iotdb-connector</artifactId>
      <version>0.9.0-SNAPSHOT</version>
    </dependency>
```


## spark-shell user guide

```
spark-shell --jars spark-iotdb-connector-0.9.0-SNAPSHOT.jar,tsfile-0.9.0-SNAPSHOT-jar-with-dependencies.jar,iotdb-jdbc-0.9.0-SNAPSHOT-jar-with-dependencies.jar

val df = spark.read.format("org.apache.iotdb.tsfile").option("url","jdbc:iotdb://127.0.0.1:6667/").option("sql","select * from root").load

df.printSchema()

df.show()
```

## if you want to partition your rdd, you can do as following
```
spark-shell --jars spark-iotdb-connector-0.9.0-SNAPSHOT.jar,tsfile-0.9.0-SNAPSHOT-jar-with-dependencies.jar,iotdb-jdbc-0.9.0-SNAPSHOT-jar-with-dependencies.jar

val df = spark.read.format("org.apache.iotdb.tsfile").option("url","jdbc:iotdb://127.0.0.1:6667/").option("sql","select * from root").
                        option("lowerBound", [lower bound of time that you want query(include)]).option("upperBound", [upper bound of time that you want query(include)]).
                        option("numPartition", [the partition number you want]).load

df.printSchema()

df.show()
```