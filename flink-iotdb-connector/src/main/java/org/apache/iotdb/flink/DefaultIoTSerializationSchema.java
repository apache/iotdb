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

package org.apache.iotdb.flink;

import java.util.Map;

public class DefaultIoTSerializationSchema implements IoTSerializationSchema<Map<String,String>> {
    public static final String FIELD_DEVICE = "device";
    public static final String FIELD_TIMESTAMP = "timestamp";
    public static final String FIELD_MEASUREMENT = "measurement";
    public static final String FIELD_VALUE = "value";

    @Override
    public Event serialize(Map<String,String> tuple) {
        if (tuple == null) {
            return null;
        }

        String device = tuple.get(FIELD_DEVICE);
        String ts = tuple.get(FIELD_TIMESTAMP);
        Long timestamp = ts == null ? System.currentTimeMillis() : Long.parseLong(ts);
        String measurement = tuple.get(FIELD_MEASUREMENT);
        String value = "'" + tuple.get(FIELD_VALUE) + "'";

        return new Event(device, timestamp, measurement, value);
    }
}