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

import org.apache.iotdb.db.sink.api.Event;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AlertManagerEvent implements Event {

  private final Map<String, String> labels;

  private final Map<String, String> annotations;

  public AlertManagerEvent(String alertname) {
    this.labels = new HashMap<>();
    this.labels.put("alertname", alertname);
    this.annotations = null;
  }

  public AlertManagerEvent(String alertname, Map<String, String> extraLabels) {
    this.labels = extraLabels;
    this.labels.put("alertname", alertname);
    this.annotations = null;
  }

  public AlertManagerEvent(
      String alertname, Map<String, String> extraLabels, Map<String, String> annotations) {

    this.labels = extraLabels;
    this.labels.put("alertname", alertname);
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

  private static String fillTemplate(Map<String, String> map, String template) {
    if (template == null || map == null) return null;
    StringBuffer sb = new StringBuffer();
    Matcher m = Pattern.compile("\\{\\{\\.\\w+}}").matcher(template);
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
