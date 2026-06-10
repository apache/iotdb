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
  public static final String INVALID_LINE_PROTOCOL = "行协议格式无效，行内容：{}";
  public static final String TAGS_ERROR = "标签格式错误，行内容：{}";
  public static final String ATTRIBUTES_ERROR = "属性格式错误，行内容：{}";
  public static final String FIELDS_ERROR = "字段格式错误，行内容：{}";
  public static final String TIMESTAMP_ERROR = "时间戳格式错误，行内容：{}";

  // --- MPPPublishHandler ---
  public static final String ON_PUBLISH_EXCEPTION =
      "onPublish 执行异常，消息为 [{}]，错误：";
  public static final String PROCESS_RESULT = "处理结果：{}";

  // --- MQTTService ---
  public static final String SERVER_START_EXCEPTION = "启动服务器时发生异常";
  public static final String STOPPING_MQTT_SERVICE = "正在停止 IoTDB MQTT 服务...";
  public static final String MQTT_SERVICE_STOPPED = "IoTDB MQTT 服务已停止。";

  // --- PayloadFormatManager ---
  public static final String MQTT_DIR = "mqttDir：{}";
  public static final String PAYLOAD_FORMAT_MANAGER_INIT_ERROR =
      "MQTT PayloadFormatManager init() 出错。";
  public static final String FORMATTER_IS_NULL = "PayloadFormatManager()，formatter 为 null。";
  public static final String FIND_MQTT_PLUGIN =
      "PayloadFormatManager()，找到 MQTT Payload 插件 {}。";
  public static final String MQTT_PLUGIN_JAR_URLS = "MQTT 插件 jarURLs：{}";
  public static final String UNKNOWN_PAYLOAD_FORMAT_NAMED = "未知 payload format 名称：";

  // --- JSONPayloadFormatter ---
  public static final String PAYLOAD_INVALID = "payload 无效";

  private MqttMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_LINE_PATTERN_PARSING_FAILS_FAILED_LINE_MESSAGE_ARG_EXCEPTION_6EFB0EE2 = "The line pattern parsing fails, and the 失败 line 消息 is {} ,异常 is";
  public static final String LOG_CONNECTION_REFUSED_CLIENT_ID_MISSING_EMPTY_VALID_CLIENT_ID_REQUIRED_A566DC15 = "连接 refused: client_id is missing or 为空. A valid client_id is required to establish a 连接.";
  public static final String LOG_RECEIVE_PUBLISH_MESSAGE_CLIENTID_ARG_USERNAME_ARG_QOS_ARG_TOPIC_7E60C3A6 = "Receive publish 消息. clientId: {}, 用户name: {}, qos: {}, 主题: {}, pay加载: {}";
  public static final String LOG_MQTT_JSON_INSERT_ERROR_CODE_ARG_MESSAGE_ARG_B1A78FBD = "mqtt json insert 错误, code={}, 消息={}";
  public static final String LOG_MEET_ERROR_INSERTING_DATABASE_ARG_TABLE_ARG_TAGS_ARG_ATTRIBUTES_173457D5 = "meet 错误 when inserting 数据库 {}, table {}, tags {}, attributes {}, fields {}, at time {},，原因：";
  public static final String LOG_MEET_ERROR_INSERTING_DEVICE_ARG_MEASUREMENTS_ARG_AT_TIME_ARG_680D67D2 = "meet 错误 when inserting device {}, measurements {}, at time {},，原因：";
  public static final String LOG_START_MQTT_SERVICE_SUCCESSFULLY_LISTENING_IP_ARG_PORT_ARG_47CE46D5 = "开始 MQTT service 成功fully, listening on ip {} port {}";

}
