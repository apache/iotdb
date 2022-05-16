package org.apache.iotdb.db.engine.trigger.utils;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class HTTPConnectionPool {

  private static volatile PoolingHttpClientConnectionManager clientConnectionManager;

  private HTTPConnectionPool() {}

  public static PoolingHttpClientConnectionManager getInstance() {
    if (clientConnectionManager == null) {
      synchronized (HTTPConnectionPool.class) {
        if (clientConnectionManager == null) {
          clientConnectionManager = new PoolingHttpClientConnectionManager();
        }
      }
    }
    return clientConnectionManager;
  }
}
