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
package org.apache.iotdb.db.mqtt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The JSON payload formatter.
 * two json format supported:
 * {
 *     "device":"root.sg.d1",
 *     "timestamp":1586076045524,
 *     "measurements":["s1","s2"],
 *     "values":[0.530635,0.530635]
 * }
 *
 * {
 *     "device":"root.sg.d1",
 *     "timestamps":[1586076045524,1586076065526],
 *     "measurements":["s1","s2"],
 *     "values":[[0.530635,0.530635], [0.530655,0.530695]]
 * }
 */
public class JSONPayloadFormatter implements PayloadFormatter {
    private static final String JSON_KEY_DEVICE = "device";
    private static final String JSON_KEY_TIMESTAMP = "timestamp";
    private static final String JSON_KEY_TIMESTAMPS = "timestamps";
    private static final String JSON_KEY_MEASUREMENTS = "measurements";
    private static final String JSON_KEY_VALUES = "values";

    @Override
    public List<Message> format(ByteBuf payload) {
        if (payload == null) {
            return null;
        }
        String txt = payload.toString(StandardCharsets.UTF_8);
        JSONObject jsonObject = JSON.parseObject(txt);

        Object timestamp = jsonObject.get(JSON_KEY_TIMESTAMP);
        if (timestamp != null) {
            return Lists.newArrayList(JSON.parseObject(txt, Message.class));
        }

        String device = jsonObject.getString(JSON_KEY_DEVICE);
        JSONArray timestamps = jsonObject.getJSONArray(JSON_KEY_TIMESTAMPS);
        JSONArray measurements = jsonObject.getJSONArray(JSON_KEY_MEASUREMENTS);
        JSONArray values = jsonObject.getJSONArray(JSON_KEY_VALUES);

        List<Message> ret = new ArrayList<>();
        for (int i = 0; i < timestamps.size(); i++) {
            Long ts = timestamps.getLong(i);

            Message message = new Message();
            message.setDevice(device);
            message.setTimestamp(ts);
            message.setMeasurements(measurements.toJavaList(String.class));
            message.setValues(((JSONArray)values.get(i)).toJavaList(String.class));
            ret.add(message);
        }
        return ret;
    }

    @Override
    public String getName() {
        return "json";
    }
}
