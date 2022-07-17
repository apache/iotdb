package org.apache.iotdb.pipe.external.kafka;

import org.apache.iotdb.pipe.external.api.DataType;
import org.apache.iotdb.pipe.external.api.ExternalPipeSinkWriterStatus;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class KafkaWriter implements IExternalPipeSinkWriter {

  private final Map<String, String> kafkaParams;
  private ExternalPipeSinkWriterStatus status;
  private KafkaProducer<String, String> producer = null;

  public KafkaWriter(Map<String, String> kafkaParams) {
    this.kafkaParams = kafkaParams;
  }

  public void open() throws IOException {
    if (producer != null) {
      return;
    }
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaParams.get("brokers"));
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try {
      this.producer = new KafkaProducer<>(props);
    } catch (Exception e) {
      throw new IOException("Connection Failed!");
    }
  }

  private void insertData(String[] path, long time, Object value) throws IOException {
    try {
      String Timeseries = String.join(".", path);
      String data = "insert:" + Timeseries + ':' + time + ':' + value;
      producer.send(new ProducerRecord<>(this.kafkaParams.get("topic"), 0, "IoTDB", data));
    } catch (Exception e) {
      throw new IOException();
    }
  }

  public void insertBoolean(String[] path, long time, boolean value) throws IOException {
    insertData(path, time, value);
  }

  public void insertInt32(String[] path, long time, int value) throws IOException {
    insertData(path, time, value);
  }

  public void insertInt64(String[] path, long time, long value) throws IOException {
    insertData(path, time, value);
  }

  public void insertFloat(String[] path, long time, float value) throws IOException {
    insertData(path, time, value);
  }

  public void insertDouble(String[] path, long time, double value) throws IOException {
    insertData(path, time, value);
  }

  public void insertText(String[] path, long time, String value) throws IOException {
    insertData(path, time, value);
  }

  public void insertVector(String[] path, DataType[] dataTypes, long time, Object[] values)
      throws IOException {
    for (Object value : values) {
      insertData(path, time, value);
    }
  }

  public void delete(String[] path, long time) throws IOException {
    try {
      String Timeseries = String.join(".", path);
      String data = "delete:" + Timeseries + ':' + time;
      producer.send(new ProducerRecord<>(this.kafkaParams.get("topic"), 0, "IoTDB", data));
    } catch (Exception e) {
      throw new IOException();
    }
  }

  // The "vector" TimeSeries does not contain its dataTypes currently.
  public void createTimeSeries(String[] path, DataType dataType) throws IOException {
    try {
      String Timeseries = String.join(".", path);
      String data = "create:" + Timeseries + ':' + dataType;
      producer.send(new ProducerRecord<>(this.kafkaParams.get("topic"), 0, "IoTDB", data));
    } catch (Exception e) {
      throw new IOException();
    }
  }

  public void deleteTimeSeries(String[] path) throws IOException {
    try {
      String Timeseries = String.join(".", path);
      String data = "del_time:" + Timeseries;
      producer.send(new ProducerRecord<>(this.kafkaParams.get("topic"), 0, "IoTDB", data));
    } catch (Exception e) {
      throw new IOException();
    }
  }

  public void flush() throws IOException {}

  public ExternalPipeSinkWriterStatus getStatus() {
    return this.status;
  }

  @Override
  public void close() throws IOException {
    this.producer.close();
  }
}
