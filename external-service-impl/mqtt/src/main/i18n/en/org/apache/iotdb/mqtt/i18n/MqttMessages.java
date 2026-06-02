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

package org.apache.iotdb.mqtt.i18n;

public final class MqttMessages {

  // --- LinePayloadFormatter ---
  public static final String INVALID_LINE_PROTOCOL = "Invalid line protocol format ,line is {}";
  public static final String TAGS_ERROR = "The tags is error , line is {}";
  public static final String ATTRIBUTES_ERROR = "The attributes is error , line is {}";
  public static final String FIELDS_ERROR = "The fields is error , line is {}";
  public static final String TIMESTAMP_ERROR = "The timestamp is error , line is {}";

  // --- MPPPublishHandler ---
  public static final String ON_PUBLISH_EXCEPTION =
      "onPublish execution exception, msg is [{}], error is ";
  public static final String PROCESS_RESULT = "process result: {}";

  // --- MQTTService ---
  public static final String SERVER_START_EXCEPTION = "Exception while starting server";
  public static final String STOPPING_MQTT_SERVICE = "Stopping IoTDB MQTT service...";
  public static final String MQTT_SERVICE_STOPPED = "IoTDB MQTT service stopped.";

  // --- PayloadFormatManager ---
  public static final String MQTT_DIR = "mqttDir: {}";
  public static final String PAYLOAD_FORMAT_MANAGER_INIT_ERROR =
      "MQTT PayloadFormatManager init() error.";
  public static final String FORMATTER_IS_NULL = "PayloadFormatManager(), formatter is null.";
  public static final String FIND_MQTT_PLUGIN =
      "PayloadFormatManager(), find MQTT Payload Plugin {}.";
  public static final String MQTT_PLUGIN_JAR_URLS = "MQTT Plugin jarURLs: {}";

  // --- JSONPayloadFormatter ---
  public static final String PAYLOAD_INVALID = "payload is invalidate";

  private MqttMessages() {}
}
