package org.apache.iotdb.pipe.external.kafka;

import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriterFactory;

import java.util.Map;
import java.util.regex.Pattern;

public class KafkaWriterFactory implements IExternalPipeSinkWriterFactory {
  private Map<String, String> kafkaParams;

  public String getProviderName() {
    return "IoTDB";
  }

  public String getExternalPipeType() {
    return "KafkaSink";
  }

  public void validateSinkParams(Map<String, String> sinkParams) throws Exception {
    if (!sinkParams.containsKey("brokers")) {
      throw new Exception("Parameters shall contain brokers.");
    }
    if (!sinkParams.containsKey("topic")) {
      throw new Exception("Parameters shall contain kafka topic.");
    }

    String brokers = sinkParams.get("brokers");

    String ip_format =
        "^((((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))|localhost):"
            + "(\\d|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{4}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5]),)*"
            + "(((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))|localhost):"
            + "(\\d|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{4}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5])$";

    if (!Pattern.matches(ip_format, brokers)) {
      throw new Exception("Incorrect IP format.");
    }

    String int_format = "^\\d*$";
    if (sinkParams.containsKey("means")
        && sinkParams.get("means").equals("non-serial")
        && sinkParams.containsKey("partition")) {
      if (!Pattern.matches(int_format, sinkParams.get("partition"))) {
        throw new Exception("Incorrect partition format.");
      }
    }
  }

  public void initialize(Map<String, String> sinkParams) throws Exception {
    this.kafkaParams = sinkParams;
  }

  public KafkaWriter get() {
    return new KafkaWriter(this.kafkaParams);
  }
}
