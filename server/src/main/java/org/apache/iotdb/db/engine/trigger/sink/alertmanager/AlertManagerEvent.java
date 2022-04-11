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

import org.apache.iotdb.db.engine.trigger.sink.api.Event;
import org.apache.iotdb.db.engine.trigger.sink.exception.SinkException;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AlertManagerEvent implements Event {

  private static final String PARAMETER_NULL_ERROR_STR = "parameter null error";

  private static final String ALERTNAME_KEY = "alertname";

  private final Map<String, String> labels;

  private final Map<String, String> annotations;

  private static final Pattern pattern = Pattern.compile("\\{\\{\\.\\w+}}");

  public AlertManagerEvent(String alertname) throws SinkException {
    if (alertname == null) {
      throw new SinkException(PARAMETER_NULL_ERROR_STR);
    }
    this.labels = new HashMap<>();
    this.labels.put(ALERTNAME_KEY, alertname);
    this.annotations = null;
  }

  public AlertManagerEvent(String alertname, Map<String, String> extraLabels) throws SinkException {
    if (alertname == null || extraLabels == null) {
      throw new SinkException(PARAMETER_NULL_ERROR_STR);
    }
    this.labels = extraLabels;
    this.labels.put(ALERTNAME_KEY, alertname);
    this.annotations = null;
  }

  public AlertManagerEvent(
      String alertname, Map<String, String> extraLabels, Map<String, String> annotations)
      throws SinkException {
    if (alertname == null || extraLabels == null || annotations == null) {
      throw new SinkException(PARAMETER_NULL_ERROR_STR);
    }

    this.labels = extraLabels;
    this.labels.put(ALERTNAME_KEY, alertname);
    this.annotations = new HashMap<>();

    for (Map.Entry<String, String> entry : annotations.entrySet()) {
      this.annotations.put(entry.getKey(), fillTemplate(this.labels, entry.getValue()));
    }
  }

  public Map<String, String> getAnnotations() {
    return annotations;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public String toJsonString() {
    Gson gson = new Gson();
    Type gsonType = new TypeToken<Map>() {}.getType();

    StringBuilder sb = new StringBuilder();
    sb.append("{\"labels\":");

    String labelsString = gson.toJson(this.labels, gsonType);
    sb.append(labelsString);

    if (this.annotations != null) {
      String annotationsString = gson.toJson(this.annotations, gsonType);
      sb.append(",");
      sb.append("\"annotations\":");
      sb.append(annotationsString);
    }
    sb.append("}");
    return sb.toString();
  }

  private static String fillTemplate(Map<String, String> map, String template) {
    if (template == null || map == null) {
      return null;
    }
    StringBuffer sb = new StringBuffer();
    Matcher m = pattern.matcher(template);
    while (m.find()) {
      String param = m.group();
      String key = param.substring(3, param.length() - 2).trim();
      String value = map.get(key);
      m.appendReplacement(sb, value == null ? "" : value);
    }
    m.appendTail(sb);
    return sb.toString();
  }
}
