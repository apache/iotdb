<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

## Flink IoTDB 连接器

IoTDB 与 [Apache Flink](https://flink.apache.org/) 的集成. 此模块包含了 iotdb sink，允许 flink job 将时序数据写入IoTDB。

### IoTDBSink

使用 `IoTDBSink` ，您需要定义一个 `IoTDBOptions` 和一个 `IoTSerializationSchema` 实例。 `IoTDBSink` 默认每次发送一个数据，可以通过调用 `withBatchSize(int)` 进行调整。

### 示例

该示例演示了如下从一个 Flink job 中发送数据到 IoTDB server 的场景：

- 一个模拟的 Source  `SensorSource` 每秒钟产生一个数据点。

- Flink使用 `IoTDBSink` 消费产生的数据并写入 IoTDB 。

  ```java
  import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
  import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
  import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
  
  import com.google.common.collect.Lists;
  import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
  import org.apache.flink.streaming.api.functions.source.SourceFunction;
  
  import java.security.SecureRandom;
  import java.util.HashMap;
  import java.util.Map;
  import java.util.Random;
  
  public class FlinkIoTDBSink {
    public static void main(String[] args) throws Exception {
      // run the flink job on local mini cluster
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  
      IoTDBOptions options = new IoTDBOptions();
      options.setHost("127.0.0.1");
      options.setPort(6667);
      options.setUser("root");
      options.setPassword("root");
      options.setStorageGroup("root.sg");
  
      // If the server enables auto_create_schema, then we do not need to register all timeseries
      // here.
      options.setTimeseriesOptionList(
          Lists.newArrayList(
              new IoTDBOptions.TimeseriesOption(
                  "root.sg.d1.s1", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY)));
  
      IoTSerializationSchema serializationSchema = new DefaultIoTSerializationSchema();
      IoTDBSink ioTDBSink =
          new IoTDBSink(options, serializationSchema)
              // enable batching
              .withBatchSize(10)
              // how many connectons to the server will be created for each parallelism
              .withSessionPoolSize(3);
  
      env.addSource(new SensorSource())
          .name("sensor-source")
          .setParallelism(1)
          .addSink(ioTDBSink)
          .name("iotdb-sink");
  
      env.execute("iotdb-flink-example");
    }
  
    private static class SensorSource implements SourceFunction<Map<String, String>> {
      boolean running = true;
      Random random = new SecureRandom();
  
      @Override
      public void run(SourceContext context) throws Exception {
        while (running) {
          Map<String, String> tuple = new HashMap();
          tuple.put("device", "root.sg.d1");
          tuple.put("timestamp", String.valueOf(System.currentTimeMillis()));
          tuple.put("measurements", "s1");
          tuple.put("types", "DOUBLE");
          tuple.put("values", String.valueOf(random.nextDouble()));
  
          context.collect(tuple);
          Thread.sleep(1000);
        }
      }
  
      @Override
      public void cancel() {
        running = false;
      }
    }
  }
  
  ```

  


### 运行方法

* 启动 IoTDB server
* 运行 `org.apache.iotdb.flink.FlinkIoTDBSink.java` 将 Flink job 运行在本地的集群上。
