package org.apache.iotdb.pipe.external.kafka;

import org.apache.iotdb.pipe.external.api.DataType;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class kafkaTest {
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
  }

  @Test
  public void full_Test() {
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

      kw.delete(Timeseries, 123);
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

    sinkParams.put("means", "with-type");
    sinkParams.put("key", "test");

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

      String[] Timeseries = {"root", "vehicle", "d0", "s0"};
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

      kw2.delete(Timeseries, 123);
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
}
