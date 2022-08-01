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
    this.status = new ExternalPipeSinkWriterStatus();
    this.status.setStartTime(System.currentTimeMillis());
    this.status.setNumOfBytesTransmitted((long) 0);
    this.status.setNumOfRecordsTransmitted((long) 0);
  }

  public void open() throws IOException {
    if (this.producer != null) {
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

  private void status_update(String data) {
    this.status.setNumOfBytesTransmitted(this.status.getNumOfBytesTransmitted() + data.length());
    this.status.setNumOfRecordsTransmitted(this.status.getNumOfRecordsTransmitted() + 1);
  }

  private void kafka_send(String data) throws IOException {
    status_update(data);
    try {
      if (this.kafkaParams.containsKey("partition")) {
        this.producer.send(
            new ProducerRecord<>(
                this.kafkaParams.get("topic"),
                Integer.parseInt(this.kafkaParams.get("partition")),
                "IoTDB",
                data));
      } else if (this.kafkaParams.containsKey("key")) {
        this.producer.send(
            new ProducerRecord<>(this.kafkaParams.get("topic"), this.kafkaParams.get("key"), data));
      } else {
        this.producer.send(new ProducerRecord<>(this.kafkaParams.get("topic"), data));
      }
    } catch (Exception e) {
      throw new IOException("insertion failed, data=" + data);
    }
  }

  private void insertData(String[] path, long time, Object value, DataType type)
      throws IOException {
    String data;
    String Timeseries = String.join(".", path);
    if (this.kafkaParams.containsKey("means")
        && this.kafkaParams.get("means").equals("with-type")) {
      data = Timeseries + ':' + time + ':' + type + ':' + value;
    } else {
      data = Timeseries + ':' + time + ':' + value;
    }
    kafka_send(data);
  }

  public void insertBoolean(String[] path, long time, boolean value) throws IOException {
    insertData(path, time, value, DataType.BOOLEAN);
  }

  public void insertInt32(String[] path, long time, int value) throws IOException {
    insertData(path, time, value, DataType.INT32);
  }

  public void insertInt64(String[] path, long time, long value) throws IOException {
    insertData(path, time, value, DataType.INT64);
  }

  public void insertFloat(String[] path, long time, float value) throws IOException {
    insertData(path, time, value, DataType.FLOAT);
  }

  public void insertDouble(String[] path, long time, double value) throws IOException {
    insertData(path, time, value, DataType.DOUBLE);
  }

  public void insertText(String[] path, long time, String value) throws IOException {
    insertData(path, time, value, DataType.TEXT);
  }

  public void insertVector(String[] path, DataType[] dataTypes, long time, Object[] values)
      throws IOException {
    String Timeseries = String.join(".", path);
    String data;
    if (this.kafkaParams.containsKey("means")
        && this.kafkaParams.get("means").equals("with-type")) {
      String[] pairs = new String[values.length];
      for (int i = 0; i < values.length; ++i) {
        pairs[i] = dataTypes[i].toString() + ':' + values[i].toString();
      }
      String Values = String.join(":", pairs);
      data = Timeseries + ':' + time + ':' + DataType.VECTOR + ':' + Values;

    } else {
      String[] no_type_values = new String[values.length];
      for (int i = 0; i < values.length; ++i) {
        no_type_values[i] = values[i].toString();
      }
      String Values = String.join(":", no_type_values);
      data = Timeseries + ':' + time + ':' + Values;
    }
    kafka_send(data);
  }

  public void delete(String[] path, long time) throws IOException {
    try {
      String Timeseries = String.join(".", path);
      String data = "delete:" + Timeseries + ':' + time;
      this.producer.send(new ProducerRecord<>(this.kafkaParams.get("topic"), 0, "IoTDB", data));
    } catch (Exception e) {
      throw new IOException();
    }
  }

  // The "vector" TimeSeries does not contain its dataTypes currently.
  // This function will be fixed when ultimate version is specified.
  public void createTimeSeries(String[] path, DataType dataType) throws IOException {}

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
