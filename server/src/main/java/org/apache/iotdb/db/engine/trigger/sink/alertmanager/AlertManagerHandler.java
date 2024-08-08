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

package org.apache.iotdb.db.engine.trigger.sink.alertmanager;

import org.apache.iotdb.db.engine.trigger.sink.api.Handler;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;

public class AlertManagerHandler implements Handler<AlertManagerConfiguration, AlertManagerEvent> {

  private static CloseableHttpClient client;
  private static int referenceCount;

  private HttpPost request;

  private static synchronized void closeClient() throws IOException {
    if (--referenceCount == 0) {
      client.close();
    }
  }

  private static synchronized void openClient() {
    if (referenceCount++ == 0) {
      client = HttpClients.createDefault();
    }
  }

  @Override
  public void close() throws IOException {
    closeClient();
  }

  @Override
  public void open(AlertManagerConfiguration configuration) {
    if (this.request == null) {
      this.request = new HttpPost(configuration.getEndpoint());
      request.setHeader("Accept", "application/json");
      request.setHeader("Content-type", "application/json");
    }

    openClient();
  }

  @Override
  public void onEvent(AlertManagerEvent event) throws SinkException {
    try {
      request.setEntity(new StringEntity("[" + event.toJsonString() + "]"));

      try (CloseableHttpResponse response = client.execute(request)) {
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
          throw new SinkException(response.getStatusLine().toString());
        }
      }
    } catch (Exception e) {
      throw new SinkException(e.getMessage());
    }
  }
}
