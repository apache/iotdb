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

package org.apache.iotdb.mqtt.server;

import org.apache.iotdb.db.protocol.mqtt.Message;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatter;
import org.apache.iotdb.db.protocol.mqtt.TableMessage;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tsfile.enums.TSDataType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The Customized JSON payload formatter. one json format supported: { "time":1586076045523,
 * "deviceID":"car_1", "deviceType":"新能源车", "point":"速度", "value":80.0 }
 */
public class CustomizedJsonPayloadFormatter implements PayloadFormatter {
  private static final String JSON_KEY_TIME = "time";
  private static final String JSON_KEY_DEVICEID = "deviceID";
  private static final String JSON_KEY_DEVICETYPE = "deviceType";
  private static final String JSON_KEY_POINT = "point";
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
      return formatTableRow(topic, jsonObject);
    } else if (jsonElement.isJsonArray()) {
      JsonArray jsonArray = jsonElement.getAsJsonArray();
      List<Message> messages = new ArrayList<>();
      for (JsonElement element : jsonArray) {
        JsonObject jsonObject = element.getAsJsonObject();
        messages.addAll(formatTableRow(topic, jsonObject));
      }
      return messages;
    }
    throw new JsonParseException("payload is invalidate");
  }

  @Override
  @Deprecated
  public List<Message> format(ByteBuf payload) {
    throw new NotImplementedException();
  }

  private List<Message> formatTableRow(String topic, JsonObject jsonObject) {
    TableMessage message = new TableMessage();
    String database = !topic.contains("/") ? topic : topic.substring(0, topic.indexOf("/"));
    String table = "test_table";

    // Parsing Database Name
    message.setDatabase((database));

    // Parsing Table Name
    message.setTable(table);

    // Parsing Tags
    List<String> tagKeys = new ArrayList<>();
    tagKeys.add(JSON_KEY_DEVICEID);
    List<Object> tagValues = new ArrayList<>();
    tagValues.add(jsonObject.get(JSON_KEY_DEVICEID).getAsString());
    message.setTagKeys(tagKeys);
    message.setTagValues(tagValues);

    // Parsing Attributes
    List<String> attributeKeys = new ArrayList<>();
    List<Object> attributeValues = new ArrayList<>();
    attributeKeys.add(JSON_KEY_DEVICETYPE);
    attributeValues.add(jsonObject.get(JSON_KEY_DEVICETYPE).getAsString());
    message.setAttributeKeys(attributeKeys);
    message.setAttributeValues(attributeValues);

    // Parsing Fields
    List<String> fields = Arrays.asList(JSON_KEY_POINT);
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT);
    List<Object> values = Arrays.asList(jsonObject.get(JSON_KEY_VALUE).getAsFloat());
    message.setFields(fields);
    message.setDataTypes(dataTypes);
    message.setValues(values);

    // Parsing timestamp
    message.setTimestamp(jsonObject.get(JSON_KEY_TIME).getAsLong());
    return Lists.newArrayList(message);
  }

  @Override
  public String getName() {
    // set the value of mqtt_payload_formatter in iotdb-common.properties as the following string:
    return "CustomizedJson2Table";
  }

  @Override
  public String getType() {
    return PayloadFormatter.TABLE_TYPE;
  }
}
