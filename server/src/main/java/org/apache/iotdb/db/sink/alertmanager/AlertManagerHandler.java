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

package org.apache.iotdb.db.sink.alertmanager;

import org.apache.iotdb.db.sink.api.Handler;
import org.apache.iotdb.db.sink.exception.SinkException;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.lang.reflect.Type;
import java.util.Map;

public class AlertManagerHandler
    implements Handler<
        org.apache.iotdb.db.sink.alertmanager.AlertManagerConfiguration,
        org.apache.iotdb.db.sink.alertmanager.AlertManagerEvent> {

  private HttpPost request;

  @Override
  public void open(org.apache.iotdb.db.sink.alertmanager.AlertManagerConfiguration configuration)
      throws Exception {
    this.request = new HttpPost(configuration.getEndpoint());
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
  }

  @Override
  public void onEvent(AlertManagerEvent event) throws Exception {

    String json = eventToJson(event);

    request.setEntity(new StringEntity(json));

    try (CloseableHttpClient client = HttpClients.createDefault()) {

      CloseableHttpResponse response = client.execute(request);

      if (response.getStatusLine().getStatusCode() != 200) {
        throw new SinkException(response.getStatusLine().toString());
      }
    }
  }

  private static String eventToJson(AlertManagerEvent event) throws SinkException {
    Gson gson = new Gson();
    Type gsonType = new TypeToken<Map>() {}.getType();

    StringBuilder sb = new StringBuilder();
    sb.append("[{\"labels\":");

    if (event.getLabels() == null) {
      throw new SinkException("labels empty error");
    }

    String labelsString = gson.toJson(event.getLabels(), gsonType);
    sb.append(labelsString);

    if (event.getAnnotations() != null) {
      String annotationsString = gson.toJson(event.getAnnotations(), gsonType);
      sb.append(",");
      sb.append("\"annotations\":");
      sb.append(annotationsString);
    }
    sb.append("}]");
    return sb.toString();
  }
}
