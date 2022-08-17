package org.apache.iotdb.pipe.external.kafka;

import org.apache.iotdb.pipe.external.api.DataType;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class kafkaTest {

  private boolean stop = false;

  @Test
  public void get_Test() {
    KafkaWriterFactory kf = new KafkaWriterFactory();
    System.out.println(kf.getProviderName());
    System.out.println(kf.getExternalPipeType());
  }

  @Test
  public void valid_Test() {
    Map<String, String> sinkParams = new HashMap<>();
    KafkaWriterFactory kf = new KafkaWriterFactory();

    sinkParams.put("brokers", "localhost:8000");
    try {
      kf.validateSinkParams(sinkParams);
      System.out.println("Correct!\nbrokers/localhost:8000, no_topic");
    } catch (Exception e) {
      System.out.println(e.getMessage() + "\nbrokers/localhost:8000, no_topic");
    }

    sinkParams.put("topic", "IoTDB");
    try {
      kf.validateSinkParams(sinkParams);
      System.out.println("Correct!\nbrokers/localhost:8000, topic/IoTDB");
    } catch (Exception e) {
      System.out.println(e.getMessage() + "\nbrokers/localhost:8000, topic/IoTDB");
    }

    sinkParams.put("brokers", "1.1.1.1:8000");
    try {
      kf.validateSinkParams(sinkParams);
      System.out.println("Correct!\nbrokers/1.1.1.1:8000, topic/IoTDB");
    } catch (Exception e) {
      System.out.println(e.getMessage() + "\nbrokers/1.1.1.1:8000, topic/IoTDB");
    }

    sinkParams.put("brokers", "1.1.1.1:8000,localhost:8000");
    try {
      kf.validateSinkParams(sinkParams);
      System.out.println("Correct!\nbrokers/1.1.1.1:8000,localhost:8000, topic/IoTDB");
    } catch (Exception e) {
      System.out.println(e.getMessage() + "\nbrokers/1.1.1.1:8000,localhost:8000, topic/IoTDB");
    }

    sinkParams.put("brokers", "1.1.1.x:8000,localhost:8000");
    try {
      kf.validateSinkParams(sinkParams);
      System.out.println("Correct!\nbrokers/1.1.1.x:8000,localhost:8000, topic/IoTDB");
    } catch (Exception e) {
      System.out.println(e.getMessage() + "\nbrokers/1.1.1.x:8000,localhost:8000, topic/IoTDB");
    }

    sinkParams.put("brokers", "1.1.1.288:8000,localhost:8000");
    try {
      kf.validateSinkParams(sinkParams);
      System.out.println("Correct!\nbrokers/1.1.1.288:8000,localhost:8000, topic/IoTDB");
    } catch (Exception e) {
      System.out.println(e.getMessage() + "\nbrokers/1.1.1.288:8000,localhost:8000, topic/IoTDB");
    }

    sinkParams.put("brokers", "1.1.1.1:8000,localhost:66666");
    try {
      kf.validateSinkParams(sinkParams);
      System.out.println("Correct!\nbrokers/1.1.1.1:8000,localhost:66666, topic/IoTDB");
    } catch (Exception e) {
      System.out.println(e.getMessage() + "\nbrokers/1.1.1.1:8000,localhost:66666, topic/IoTDB");
    }

    sinkParams.put("brokers", "1.1.1.1:8000,localhost:65535");
    sinkParams.put("means", "no-type");
    sinkParams.put("partition", "0");
    try {
      kf.validateSinkParams(sinkParams);
      System.out.println(
          "Correct!\nbrokers/1.1.1.1:8000,localhost:65535, topic/IoTDB, means/non-serial, partition/0");
    } catch (Exception e) {
      System.out.println(
          e.getMessage()
              + "\nbrokers/1.1.1.1:8000,localhost:65535, topic/IoTDB, means/non-serial, partition/0");
    }

    sinkParams.put("partition", "x");
    try {
      kf.validateSinkParams(sinkParams);
      System.out.println(
          "Correct!\nbrokers/1.1.1.1:8000,localhost:65535, topic/IoTDB, means/non-serial, partition/x");
    } catch (Exception e) {
      System.out.println(
          e.getMessage()
              + "\nbrokers/1.1.1.1:8000,localhost:65535, topic/IoTDB, means/non-serial, partition/x");
    }

    sinkParams.put("means", "with-type");
    try {
      kf.validateSinkParams(sinkParams);
      System.out.println(
          "Correct!\nbrokers/1.1.1.1:8000,localhost:65535, topic/IoTDB, means/serial, partition/x");
    } catch (Exception e) {
      System.out.println(
          e.getMessage()
              + "\nbrokers/1.1.1.1:8000,localhost:65535, topic/IoTDB, means/serial, partition/x");
    }

    Map<String, String> sourceParams = new HashMap<>();
    ConsumerValidator cv = new ConsumerValidator();
    sourceParams.put("brokers", "localhost:8000");
    sourceParams.put("topic", "IoTDB");
    sourceParams.put("offset", "earliest");
    try {
      cv.validate_params(sourceParams);
      System.out.println("Correct!\nbrokers/localhost:8000, topic/IoTDB, offset/earliest");
    } catch (Exception e) {
      System.out.println(e.getMessage() + "\nbrokers/localhost:8000, topic/IoTDB, offset/earliest");
    }

    sourceParams.put("offset", "latest");
    try {
      cv.validate_params(sourceParams);
      System.out.println("Correct!\nbrokers/localhost:8000, topic/IoTDB, offset/latest");
    } catch (Exception e) {
      System.out.println(e.getMessage() + "\nbrokers/localhost:8000, topic/IoTDB, offset/latest");
    }

    sourceParams.put("offset", "none");
    try {
      cv.validate_params(sourceParams);
      System.out.println("Correct!\nbrokers/localhost:8000, topic/IoTDB, offset/none");
    } catch (Exception e) {
      System.out.println(e.getMessage() + "\nbrokers/localhost:8000, topic/IoTDB, offset/none");
    }
  }

  private void send_random_point(int message_num, int point_num) throws IOException {

    KafkaProducer<String, String> producer;
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try {
      producer = new KafkaProducer<>(props);
    } catch (Exception e) {
      throw new IOException("Connection Failed!");
    }

    String prefix = "root.test";

    for (int i = 0; i < message_num; ++i) {
      String[] devices = new String[point_num];
      String[] values = new String[point_num];
      String path = prefix + ".r_0" + ".d_" + i/1000000;
      for (int j = 0; j < point_num; ++j) {
        devices[j] = "s_" + j;
        values[j] = String.valueOf(j);
      }
      String data = path + ',' + i + ',' + String.join(":", devices) + ',' + String.join(":", values);
      producer.send(new ProducerRecord<>("IoTDB", data));
    }

    producer.close();
  }

  private void send_to_kafka_no_type() {
    Map<String, String> sinkParams = new HashMap<>();
    KafkaWriterFactory kf = new KafkaWriterFactory();

    sinkParams.put("brokers", "localhost:9092");
    sinkParams.put("topic", "IoTDB");

    try {
      kf.validateSinkParams(sinkParams);
      System.out.println("Correct!\nbrokers/localhost:9092, topic/IoTDB, means/serial");
    } catch (Exception e) {
      System.out.println(e.getMessage() + "\nbrokers/localhost:9092, topic/IoTDB, means/serial");
    }

    try {
      kf.initialize(sinkParams);
    } catch (Exception e) {
      System.out.println("?? Why initializing should cause exception here");
    }

    KafkaWriter kw = kf.get();
    try {
      kw.open();

      String[] Timeseries = {"root", "vehicle", "d0", "s0"};
      kw.createTimeSeries(Timeseries, DataType.BOOLEAN);
      kw.insertBoolean(Timeseries, 123, true);

      Timeseries[3] = "s1";
      kw.createTimeSeries(Timeseries, DataType.TEXT);
      kw.insertText(Timeseries, 123, "txt");

      Timeseries[3] = "s2";
      kw.createTimeSeries(Timeseries, DataType.INT32);
      kw.insertInt32(Timeseries, 123, 15);

      Timeseries[3] = "s3";
      kw.createTimeSeries(Timeseries, DataType.INT64);
      kw.insertInt64(Timeseries, 123, 166666);

      Timeseries[3] = "s4";
      kw.createTimeSeries(Timeseries, DataType.FLOAT);
      kw.insertFloat(Timeseries, 123, 1.5f);

      Timeseries[3] = "s5";
      kw.createTimeSeries(Timeseries, DataType.DOUBLE);
      kw.insertDouble(Timeseries, 123, 1.33);

      Timeseries[3] = "s1";
      kw.createTimeSeries(Timeseries, DataType.TEXT);
      kw.insertText(Timeseries, 124, "txt");

      kw.delete(Timeseries, 124);

      Timeseries[3] = "s2";
      kw.deleteTimeSeries(Timeseries);

      // String[] Timeseries2 = {"root", "a", "c"};
      // kw.createTimeSeries(Timeseries2, DataType.VECTOR);
      //      kw.insertVector(
      //          Timeseries2,
      //          new DataType[] {DataType.DOUBLE, DataType.TEXT},
      //          133,
      //          new Object[] {12.3, "testing"});

      kw.flush();
      System.out.println("\n\n-----------------------------------------\n\n");
      System.out.println("status" + kw.getStatus());
      System.out.println("\n\n-----------------------------------------\n\n");
      kw.close();
    } catch (Exception e) {
      System.out.println("Pipe writing failed!");
    }
  }

  private void send_to_kafka_with_type() {
    Map<String, String> sinkParams = new HashMap<>();
    KafkaWriterFactory kf = new KafkaWriterFactory();

    sinkParams.put("brokers", "localhost:9092");
    sinkParams.put("topic", "IoTDB");
    sinkParams.put("means", "with-type");

    try {
      kf.validateSinkParams(sinkParams);
      System.out.println("Correct!\nbrokers/localhost:9092, topic/IoTDB, means/non-serial");
    } catch (Exception e) {
      System.out.println(
          e.getMessage() + "\nbrokers/localhost:9092, topic/IoTDB, means/non-serial");
    }

    try {
      kf.initialize(sinkParams);
    } catch (Exception e) {
      System.out.println("?? Why initializing should cause exception here");
    }

    KafkaWriter kw2 = kf.get();
    try {
      kw2.open();

      String[] Timeseries = {"root", "vehicle", "d1", "s0"};
      kw2.createTimeSeries(Timeseries, DataType.BOOLEAN);
      kw2.insertBoolean(Timeseries, 123, true);

      Timeseries[3] = "s1";
      kw2.createTimeSeries(Timeseries, DataType.TEXT);
      kw2.insertText(Timeseries, 123, "txt");

      Timeseries[3] = "s2";
      kw2.createTimeSeries(Timeseries, DataType.INT32);
      kw2.insertInt32(Timeseries, 123, 15);

      Timeseries[3] = "s3";
      kw2.createTimeSeries(Timeseries, DataType.INT64);
      kw2.insertInt64(Timeseries, 123, 166666);

      Timeseries[3] = "s4";
      kw2.createTimeSeries(Timeseries, DataType.FLOAT);
      kw2.insertFloat(Timeseries, 123, 1.5f);

      Timeseries[3] = "s5";
      kw2.createTimeSeries(Timeseries, DataType.DOUBLE);
      kw2.insertDouble(Timeseries, 123, 1.33);

      Timeseries[3] = "s1";
      kw2.createTimeSeries(Timeseries, DataType.TEXT);
      kw2.insertText(Timeseries, 124, "txt");

      kw2.delete(Timeseries, 124);

      Timeseries[3] = "s2";
      kw2.deleteTimeSeries(Timeseries);

      // String[] Timeseries2 = {"root", "a", "c"};
      // kw2.createTimeSeries(Timeseries2, DataType.VECTOR);
      //      kw2.insertVector(
      //          Timeseries2,
      //          new DataType[] {DataType.DOUBLE, DataType.TEXT},
      //          133,
      //          new Object[] {12.3, "testing"});

      kw2.flush();
      System.out.println("\n\n-----------------------------------------\n\n");
      System.out.println("status" + kw2.getStatus());
      System.out.println("\n\n-----------------------------------------\n\n");
      kw2.close();
    } catch (Exception e) {
      System.out.println("Pipe writing failed!");
    }
  }

  private void load_from_kafka() {
    Map<String, String> sourceParams = new HashMap<>();
    ConsumerValidator cv = new ConsumerValidator();
    sourceParams.put("brokers", "localhost:9092");
    sourceParams.put("topic", "IoTDB");
    sourceParams.put("offset", "earliest");

    try {
      cv.validate_params(sourceParams);
    } catch (Exception e) {
      e.printStackTrace();
    }

    SessionPool sp = new SessionPool("localhost", 6667, "root", "root", 5);
    KafkaLoader kl = new KafkaLoader(sp, sourceParams);

    System.out.println("created:");
    System.out.println("status:" + kl.getStatus());
    kl.open();
    System.out.println("open:");
    System.out.println("status:" + kl.getStatus());
    System.out.println(kl);

    kl.run();
    /*System.out.println("run:");
    System.out.println("status:" + kl.getStatus());
    try {
      Thread.sleep(3000);
    } catch (InterruptedException ignore) {
    }
    System.out.println(kl);

    kl.cancel();
    System.out.println("cancel:");
    System.out.println("status:" + kl.getStatus());
    try {
      Thread.sleep(3000);
    } catch (InterruptedException ignore) {
    }
    System.out.println(kl);

    kl.run();
    System.out.println("run:");
    System.out.println("status:" + kl.getStatus());
    try {
      Thread.sleep(3000);
    } catch (InterruptedException ignore) {
    }
    System.out.println(kl);*/

    while (true) {
      try {
        Thread.sleep(10000);
        System.out.println("\n");
        System.out.println(kl);
        System.out.println("\n");
      } catch (InterruptedException ignore) {
      }
    }
    /* kl.close();
    System.out.println("close:");
    System.out.println("status:" + kl.getStatus());
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ignore) {
    }
    System.out.println(kl);*/
  }

  @Test
  public void full_Test() {
    // send_to_kafka_no_type();
    // send_to_kafka_with_type();
    try {
      send_random_point(1000000, 200);
    } catch (IOException ignore) {
    }
    load_from_kafka();
  }
}
