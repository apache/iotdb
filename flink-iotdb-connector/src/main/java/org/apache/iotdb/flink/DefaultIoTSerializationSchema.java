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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultIoTSerializationSchema implements IoTSerializationSchema<Map<String,String>> {
    public static final String FIELD_DEVICE = "device";
    public static final String FIELD_TIMESTAMP = "timestamp";
    public static final String FIELD_MEASUREMENTS = "measurements";
    public static final String FIELD_VALUES = "values";
    public static final String DEFAULT_SEPARATOR = ",";

    private Map<String, IoTDBOptions.TimeseriesOption> timeseriesOptionMap;

    public DefaultIoTSerializationSchema(IoTDBOptions ioTDBOptions) {
        timeseriesOptionMap = new HashMap<>();
        for (IoTDBOptions.TimeseriesOption timeseriesOption : ioTDBOptions.getTimeseriesOptionList()) {
            timeseriesOptionMap.put(timeseriesOption.getPath(), timeseriesOption);
        }
    }

    @Override
    public Event serialize(Map<String,String> tuple) {
        if (tuple == null) {
            return null;
        }

        String device = tuple.get(FIELD_DEVICE);

        String ts = tuple.get(FIELD_TIMESTAMP);
        Long timestamp = ts == null ? System.currentTimeMillis() : Long.parseLong(ts);

        List<String> measurements = null;
        if (tuple.get(FIELD_MEASUREMENTS) != null) {
            measurements = Arrays.asList(tuple.get(FIELD_MEASUREMENTS).split(DEFAULT_SEPARATOR));
        }

        List<String> values = null;
        if (tuple.get(FIELD_VALUES) != null) {
            values = Arrays.asList(tuple.get(FIELD_VALUES).split(DEFAULT_SEPARATOR));
        }

        if (device != null && measurements != null && values != null && measurements.size() == values.size()) {
            for (int i = 0; i < measurements.size(); i++) {
                String measurement = device + "." + measurements.get(i);
                IoTDBOptions.TimeseriesOption timeseriesOption = timeseriesOptionMap.get(measurement);
                if (timeseriesOption!= null && TSDataType.TEXT.equals(timeseriesOption.getDataType())) {
                    // The TEXT data type should be covered by " or '
                    values.set(i, "'" + values.get(i) + "'");
                }
            }
        }

        return new Event(device, timestamp, measurements, values);
    }
}