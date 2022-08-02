package org.apache.iotdb.pipe.external.kafka;

import java.util.Map;
import java.util.regex.Pattern;

public class ConsumerValidator {
  public void validate_params(Map<String, String> kafkaParams) throws Exception {
    if (!kafkaParams.containsKey("brokers")) {
      throw new Exception("Parameters shall contain brokers.");
    }
    if (!kafkaParams.containsKey("topic")) {
      throw new Exception("Parameters shall contain kafka topic.");
    }

    String brokers = kafkaParams.get("brokers");

    String ip_format =
        "^((((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))|localhost):"
            + "(\\d|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{4}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5]),)*"
            + "(((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))|localhost):"
            + "(\\d|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{4}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5])$";

    if (!Pattern.matches(ip_format, brokers)) {
      throw new Exception("Incorrect IP format.");
    }

    if (kafkaParams.containsKey("offset")) {
      String offset = kafkaParams.get("offset");
      if (!offset.equals("earliest") && !offset.equals("latest")) {
        throw new Exception("Offset shall be either earliest or latest.");
      }
    }
  }
}
