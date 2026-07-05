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
  public static final String UNKNOWN_PAYLOAD_FORMAT_NAMED = "Unknown payload format named: ";

  // --- JSONPayloadFormatter ---
  public static final String PAYLOAD_INVALID = "payload is invalidate";

  private MqttMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_LINE_PATTERN_PARSING_FAILS_FAILED_LINE_MESSAGE_ARG_EXCEPTION_6EFB0EE2 = "The line pattern parsing fails, and the failed line message is {} ,exception is";
  public static final String LOG_CONNECTION_REFUSED_CLIENT_ID_MISSING_EMPTY_VALID_CLIENT_ID_REQUIRED_A566DC15 =
      "Connection refused: client_id is missing or empty. A valid client_id is required to establish"
      + " a connection.";
  public static final String LOG_RECEIVE_PUBLISH_MESSAGE_CLIENTID_ARG_USERNAME_ARG_QOS_ARG_TOPIC_7E60C3A6 = "Receive publish message. clientId: {}, username: {}, qos: {}, topic: {}, payload: {}";
  public static final String LOG_MQTT_JSON_INSERT_ERROR_CODE_ARG_MESSAGE_ARG_B1A78FBD = "mqtt json insert error, code={}, message={}";
  public static final String LOG_MEET_ERROR_INSERTING_DATABASE_ARG_TABLE_ARG_TAGS_ARG_ATTRIBUTES_173457D5 =
      "meet error when inserting database {}, table {}, tags {}, attributes {}, fields {}, at time"
      + " {}, because ";
  public static final String LOG_MEET_ERROR_INSERTING_DEVICE_ARG_MEASUREMENTS_ARG_AT_TIME_ARG_680D67D2 = "meet error when inserting device {}, measurements {}, at time {}, because ";
  public static final String LOG_START_MQTT_SERVICE_SUCCESSFULLY_LISTENING_IP_ARG_PORT_ARG_47CE46D5 = "Start MQTT service successfully, listening on ip {} port {}";

}
