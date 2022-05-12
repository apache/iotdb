/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.trigger.sink.http;

import org.apache.iotdb.db.engine.trigger.sink.api.Handler;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.util.List;

public class HTTPHandler implements Handler<HTTPConfiguration, HTTPEvent> {
  private static final PoolingHttpClientConnectionManager clientConnectionManager;
  private static CloseableHttpClient client;
  private static int referenceCount;

  private HttpPost request;
  private HTTPConfiguration configuration;

  static {
    // Create connection-pool manager
    clientConnectionManager = new PoolingHttpClientConnectionManager();
    // Set the max number of connections
    clientConnectionManager.setMaxTotal(200);
    // Set the maximum number of connections per host and the specified number of connections
    // per website, which will not affect the access of other websites
    clientConnectionManager.setDefaultMaxPerRoute(20);
  }

  private static synchronized void closeClient() throws IOException {
    if (--referenceCount == 0) {
      client.close();
    }
  }

  private static synchronized void openClient() {
    if (referenceCount++ == 0) {
      client = HttpClients.custom().setConnectionManager(clientConnectionManager).build();
    }
  }

  @Override
  public void close() throws IOException {
    closeClient();
  }

  @Override
  public void open(HTTPConfiguration configuration) {
    this.configuration = configuration;
    if (this.request == null) {
      this.request = new HttpPost(configuration.getEndpoint());
      request.setHeader("Accept", "application/json");
      request.setHeader("Content-type", "application/json");
    }

    openClient();
  }

  @Override
  public void onEvent(HTTPEvent event) throws SinkException {
    CloseableHttpResponse response = null;
    try {
      request.setEntity(new StringEntity("[" + event.toJsonString() + "]"));
      response = client.execute(request);
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new SinkException(response.getStatusLine().toString());
      }
    } catch (Exception e) {
      if (configuration.isStopForwardingIfException()) {
        throw new SinkException("Forward Exception", e);
      }
      e.printStackTrace();
    } finally {
      try {
        if (null != response) {
          response.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void onEvent(List<HTTPEvent> events) throws SinkException {
    CloseableHttpResponse response = null;
    try {
      StringBuilder sb = new StringBuilder();
      for (HTTPEvent event : events) {
        sb.append(event.toJsonString()).append(", ");
      }
      sb.replace(sb.lastIndexOf(", "), sb.length() - 1, "");
      request.setEntity(new StringEntity("[" + sb + "]"));
      response = client.execute(request);
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new SinkException(response.getStatusLine().toString());
      }
    } catch (Exception e) {
      if (configuration.isStopForwardingIfException()) {
        throw new SinkException("Forward Exception", e);
      }
      e.printStackTrace();
    } finally {
      try {
        if (null != response) {
          response.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
