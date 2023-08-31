/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.mqtt;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The SparkplugBPayloadFormatter is a special form of JSONPayloadFormatter that is able to process
 * messages in the SparkplugB format.
 */
public class SparkplugBPayloadFormatter implements PayloadFormatter {
  private static final String JSON_KEY_TIMESTAMP = "timestamp";
  private static final String JSON_KEY_MEASUREMENTS = "metrics";
  private static final String JSON_KEY_NAME = "name";
  private static final String JSON_KEY_DATATYPE = "dataType";
  private static final String JSON_KEY_VALUE = "value";
  private static final Gson GSON = new GsonBuilder().create();

  @Override
  public List<Message> format(String topic, ByteBuf payload) {
    if (payload == null) {
      return new ArrayList<>();
    }
    String txt = payload.toString(StandardCharsets.UTF_8);
    JsonElement jsonElement = GSON.fromJson(txt, JsonElement.class);
    if (jsonElement.isJsonObject()) {
      JsonObject jsonObject = jsonElement.getAsJsonObject();
      if (jsonObject.get(JSON_KEY_TIMESTAMP) != null) {
        return formatJson(topic, jsonObject);
      }
    }
    throw new JsonParseException("invalid payload");
  }

  private List<Message> formatJson(String topic, JsonObject jsonObject) {
    Map<Long, Message> messages = new TreeMap<>();
    // Replace the prefix of "spB1.0" and replace that with "root" and replace all
    // segments of the MQTT topic path with escaped versions for IoTDB
    String deviceId = "root.`" + topic.substring(topic.indexOf("/") + 1).replace("/", "`.`") + "`";
    long messageTimestamp = jsonObject.get(JSON_KEY_TIMESTAMP).getAsLong();
    JsonArray measurements = jsonObject.get(JSON_KEY_MEASUREMENTS).getAsJsonArray();
    measurements
        .iterator()
        .forEachRemaining(
            measurement -> {
              JsonObject asJsonObject = measurement.getAsJsonObject();
              // Get the timestamp for the current measurement, if none exists, use that of the
              // message.
              long measurementTimestamp = messageTimestamp;
              if (asJsonObject.has(JSON_KEY_TIMESTAMP)) {
                measurementTimestamp = asJsonObject.get(JSON_KEY_TIMESTAMP).getAsLong();
              }
              String measurementName = asJsonObject.get(JSON_KEY_NAME).getAsString();
              String sparkplugBDataType =
                  asJsonObject.get(JSON_KEY_DATATYPE).getAsString().toUpperCase();
              TSDataType measurementDataType =
                  getTSDataTypeForSparkplugBDataType(sparkplugBDataType);
              String measurementValue = asJsonObject.get(JSON_KEY_VALUE).getAsString();

              // Group together measurements of the same time.
              if (!messages.containsKey(measurementTimestamp)) {
                Message newMessage = new Message();
                newMessage.setTimestamp(measurementTimestamp);
                newMessage.setDevice(deviceId);
                newMessage.setMeasurements(new ArrayList<>());
                newMessage.setDataTypes(new ArrayList<>());
                newMessage.setValues(new ArrayList<>());
                messages.put(measurementTimestamp, newMessage);
              }
              Message curMessage = messages.get(measurementTimestamp);
              curMessage.getMeasurements().add(measurementName);
              curMessage.getDataTypes().add(measurementDataType);
              curMessage.getValues().add(measurementValue);
            });
    return new ArrayList<>(messages.values());
  }

  @Override
  public String getName() {
    return "json";
  }

  /**
   * Map the data types used in Sparkplug to the ones used in IoTDB.
   *
   * @param sparkplugBDataType string representation of the sparkplug datatype
   * @return TSDataType used in IoTDB
   */
  protected TSDataType getTSDataTypeForSparkplugBDataType(String sparkplugBDataType) {
    if (sparkplugBDataType == null) {
      return TSDataType.TEXT;
    }
    switch (sparkplugBDataType) {
      case "BYTES":
      case "DATASET":
      case "DATETIME":
      case "FILE":
      case "STRING":
      case "TEXT":
      case "UNKNOWN":
      case "UUID":
      case "TEMPLATE":
        return TSDataType.TEXT;
      case "INT8":
      case "INT16":
      case "INT32":
      case "UINT8":
        return TSDataType.INT32;
      case "INT64":
      case "UINT16":
      case "UINT32":
      case "UINT64":
        return TSDataType.INT64;
      case "FLOAT":
        return TSDataType.FLOAT;
      case "DOUBLE":
        return TSDataType.DOUBLE;
      case "BOOLEAN":
        return TSDataType.BOOLEAN;
    }
    return TSDataType.TEXT;
  }
}
