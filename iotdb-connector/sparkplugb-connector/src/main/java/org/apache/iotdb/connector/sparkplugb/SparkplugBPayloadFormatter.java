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

package org.apache.iotdb.connector.sparkplugb;

import org.apache.iotdb.db.protocol.mqtt.Message;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import org.eclipse.tahu.protobuf.SparkplugBProto;

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

  @Override
  public String getName() {
    return "spB1.0";
  }

  @Override
  public List<Message> format(String topic, ByteBuf rawPayload) {
    if (rawPayload == null) {
      return new ArrayList<>();
    }
    try {
      SparkplugBProto.Payload payload = SparkplugBProto.Payload.parseFrom(rawPayload.array());
      return processMessage(topic, payload);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Error parsing protobuf payload", e);
    }
  }

  private List<Message> processMessage(String topic, SparkplugBProto.Payload payload) {
    Map<Long, Message> messages = new TreeMap<>();
    // Replace the prefix of "spB1.0" and replace that with "root" and replace all
    // segments of the MQTT topic path with escaped versions for IoTDB
    String deviceId = "root.`" + topic.substring(topic.indexOf("/") + 1).replace("/", "`.`") + "`";
    long messageTimestamp = payload.getTimestamp();
    for (SparkplugBProto.Payload.Metric metric : payload.getMetricsList()) {
      // Get the timestamp for the current measurement, if none exists, use that of the
      // message.
      long measurementTimestamp = messageTimestamp;
      if (metric.hasTimestamp()) {
        measurementTimestamp = metric.getTimestamp();
      }
      String measurementName = metric.getName();
      int sparkplugBDataType = metric.getDatatype();
      TSDataType measurementDataType = getTSDataTypeForSparkplugBDataType(sparkplugBDataType);
      String measurementValue = metric.getStringValue();

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
    }
    return new ArrayList<>(messages.values());
  }

  /**
   * Map the data types used in Sparkplug to the ones used in IoTDB.
   *
   * @param sparkplugBDataType string representation of the sparkplug datatype
   * @return TSDataType used in IoTDB
   */
  protected TSDataType getTSDataTypeForSparkplugBDataType(int sparkplugBDataType) {
    SparkplugBProto.DataType dataType = SparkplugBProto.DataType.forNumber(sparkplugBDataType);
    if (dataType == null) {
      return TSDataType.TEXT;
    }
    switch (dataType) {
      case Bytes:
      case DataSet:
      case DateTime:
      case File:
      case String:
      case Text:
      case Unknown:
      case UUID:
      case Template:
        return TSDataType.TEXT;
      case Int8:
      case Int16:
      case Int32:
      case UInt8:
      case UInt16:
        return TSDataType.INT32;
      case Int64:
      case UInt32:
      case UInt64:
        return TSDataType.INT64;
      case Float:
        return TSDataType.FLOAT;
      case Double:
        return TSDataType.DOUBLE;
      case Boolean:
        return TSDataType.BOOLEAN;
    }
    return TSDataType.TEXT;
  }
}
