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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @inheritDoc
 * The default implementation of IoTSerializationSchema. Gets info from a map struct.
 */
public class DefaultIoTSerializationSchema implements IoTSerializationSchema<Map<String,String>> {
    private String fieldDevice = "device";
    private String fieldTimestamp = "timestamp";
    private String fieldMeasurements = "measurements";
    private String fieldValues = "values";
    private String separator = ",";

    @Override
    public Event serialize(Map<String,String> tuple) {
        if (tuple == null) {
            return null;
        }

        String device = tuple.get(fieldDevice);

        String ts = tuple.get(fieldTimestamp);
        Long timestamp = ts == null ? System.currentTimeMillis() : Long.parseLong(ts);

        List<String> measurements = null;
        if (tuple.get(fieldMeasurements) != null) {
            measurements = Arrays.asList(tuple.get(fieldMeasurements).split(separator));
        }

        List<String> values = null;
        if (tuple.get(fieldValues) != null) {
            values = Arrays.asList(tuple.get(fieldValues).split(separator));
        }

        return new Event(device, timestamp, measurements, values);
    }

    public String getFieldDevice() {
        return fieldDevice;
    }

    public void setFieldDevice(String fieldDevice) {
        this.fieldDevice = fieldDevice;
    }

    public String getFieldTimestamp() {
        return fieldTimestamp;
    }

    public void setFieldTimestamp(String fieldTimestamp) {
        this.fieldTimestamp = fieldTimestamp;
    }

    public String getFieldMeasurements() {
        return fieldMeasurements;
    }

    public void setFieldMeasurements(String fieldMeasurements) {
        this.fieldMeasurements = fieldMeasurements;
    }

    public String getFieldValues() {
        return fieldValues;
    }

    public void setFieldValues(String fieldValues) {
        this.fieldValues = fieldValues;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }
}
