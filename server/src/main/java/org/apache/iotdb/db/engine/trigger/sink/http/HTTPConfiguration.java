package org.apache.iotdb.db.engine.trigger.sink.http;

import org.apache.iotdb.db.engine.trigger.sink.api.Configuration;

public class HTTPConfiguration implements Configuration {
  private final String endpoint;

  public HTTPConfiguration(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getEndpoint() {
    return endpoint;
  }
}
