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
  }

  @Test
  public void full_Test() {
    Map<String, String> sinkParams = new HashMap<>();
    KafkaWriterFactory kf = new KafkaWriterFactory();

    sinkParams.put("brokers", "localhost:9092");
    sinkParams.put("topic", "IoTDB");
    try {
      kf.validateSinkParams(sinkParams);
      System.out.println("Correct!\nbrokers/localhost:8000, topic/IoTDB");
    } catch (Exception e) {
      System.out.println(e.getMessage() + "\nbrokers/localhost:8000, topic/IoTDB");
    }

    try {
      kf.initialize(sinkParams);
    } catch (Exception e) {
      System.out.println("?? Why initializing should cause exception here");
    }

    KafkaWriter kw = kf.get();
    try {
      kw.open();
      String[] Timeseries = {"root", "a", "b"};
      kw.createTimeSeries(Timeseries, DataType.BOOLEAN);
      kw.insertBoolean(Timeseries, 123, true);
      kw.delete(Timeseries, 123);
      kw.deleteTimeSeries(Timeseries);
      kw.flush();
      kw.close();
    } catch (Exception e) {
      System.out.println("Pipe writing failed!");
    }
  }
}
