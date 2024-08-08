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

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The JSON payload formatter. two json format supported: { "device":"root.sg.d1",
 * "timestamp":1586076045524, "measurements":["s1","s2"], "values":[0.530635,0.530635] }
 *
 * <p>{ "device":"root.sg.d1", "timestamps":[1586076045524,1586076065526],
 * "measurements":["s1","s2"], "values":[[0.530635,0.530635], [0.530655,0.530695]] }
 */
public class JSONPayloadFormatter implements PayloadFormatter {
  private static final String JSON_KEY_DEVICE = "device";
  private static final String JSON_KEY_TIMESTAMP = "timestamp";
  private static final String JSON_KEY_TIMESTAMPS = "timestamps";
  private static final String JSON_KEY_MEASUREMENTS = "measurements";
  private static final String JSON_KEY_VALUES = "values";
  private static final Gson GSON = new GsonBuilder().create();

  @Override
  public List<Message> format(ByteBuf payload) {
    if (payload == null) {
      return null;
    }
    String txt = payload.toString(StandardCharsets.UTF_8);
    JsonElement jsonElement = GSON.fromJson(txt, JsonElement.class);
    if (jsonElement.isJsonObject()) {
      JsonObject jsonObject = jsonElement.getAsJsonObject();
      if (jsonObject.get(JSON_KEY_TIMESTAMP) != null) {
        return formatJson(jsonObject);
      }
      if (jsonObject.get(JSON_KEY_TIMESTAMPS) != null) {
        return formatBatchJson(jsonObject);
      }
    } else if (jsonElement.isJsonArray()) {
      JsonArray jsonArray = jsonElement.getAsJsonArray();
      List<Message> messages = new ArrayList<>();
      for (JsonElement element : jsonArray) {
        JsonObject jsonObject = element.getAsJsonObject();
        if (jsonObject.get(JSON_KEY_TIMESTAMP) != null) {
          messages.addAll(formatJson(jsonObject));
        }
        if (jsonObject.get(JSON_KEY_TIMESTAMPS) != null) {
          messages.addAll(formatBatchJson(jsonObject));
        }
      }
      return messages;
    }
    throw new JsonParseException("payload is invalidate");
  }

  private List<Message> formatJson(JsonObject jsonObject) {
    Message message = new Message();
    message.setDevice(jsonObject.get(JSON_KEY_DEVICE).getAsString());
    message.setTimestamp(jsonObject.get(JSON_KEY_TIMESTAMP).getAsLong());
    message.setMeasurements(
        GSON.fromJson(
            jsonObject.get(JSON_KEY_MEASUREMENTS), new TypeToken<List<String>>() {}.getType()));
    message.setValues(
        GSON.fromJson(jsonObject.get(JSON_KEY_VALUES), new TypeToken<List<String>>() {}.getType()));
    return Lists.newArrayList(message);
  }

  private List<Message> formatBatchJson(JsonObject jsonObject) {
    String device = jsonObject.get(JSON_KEY_DEVICE).getAsString();
    List<String> measurements =
        GSON.fromJson(
            jsonObject.getAsJsonArray(JSON_KEY_MEASUREMENTS),
            new TypeToken<List<String>>() {}.getType());
    List<Long> timestamps =
        GSON.fromJson(
            jsonObject.get(JSON_KEY_TIMESTAMPS), new TypeToken<List<Long>>() {}.getType());
    List<List<String>> values =
        GSON.fromJson(
            jsonObject.get(JSON_KEY_VALUES), new TypeToken<List<List<String>>>() {}.getType());

    List<Message> ret = new ArrayList<>(timestamps.size());
    for (int i = 0; i < timestamps.size(); i++) {
      Message message = new Message();
      message.setDevice(device);
      message.setTimestamp(timestamps.get(i));
      message.setMeasurements(measurements);
      message.setValues(values.get(i));
      ret.add(message);
    }
    return ret;
  }

  @Override
  public String getName() {
    return "json";
  }
}
