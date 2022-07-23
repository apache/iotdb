package org.apache.iotdb.pipe.external.kafka;

// import org.apache.iotdb.session.Session;

import java.util.Map;

public class KafkaLoader {
  // private final Session session;
  private final Map<String, String> kafkaParams;

  public KafkaLoader(Map<String, String> kafkaParams) {
    // this.session = session;
    this.kafkaParams = kafkaParams;
  }

  public void open() {}

  public void close() {}

  public void run() {}

  public void cancel() {}
}
