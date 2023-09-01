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
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatterV2;
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
public class SparkplugBPayloadFormatter implements PayloadFormatterV2 {

  @Override
  public String getName() {
    return "spB1.0";
  }

  @Override
  public List<Message> format(ByteBuf rawPayload) {
    throw new RuntimeException("SparkplugBPayloadFormatter needs V2 API");
  }

  @Override
  public List<Message> format(String topic, ByteBuf rawPayload) {
    if (rawPayload == null) {
      return new ArrayList<>();
    }
    try {
      byte[] bytes = new byte[rawPayload.readableBytes()];
      rawPayload.duplicate().readBytes(bytes);
      SparkplugBProto.Payload payload = SparkplugBProto.Payload.parseFrom(bytes);
      return processMessage(topic, payload);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Error parsing protobuf payload", e);
    }
  }

  private List<Message> processMessage(String topic, SparkplugBProto.Payload payload) {
    Map<Long, Message> messages = new TreeMap<>();
    // Replace the prefix of "spB1.0" and replace that with "root" and replace all
    // segments of the MQTT topic path with escaped versions for IoTDB
    String deviceId =
        "root.`"
            + topic.substring(topic.indexOf("/") + 1).replace("/", "`.`").replace(" ", "_")
            + "`";
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
      String measurementValue = getStringValueForMetric(metric, sparkplugBDataType);

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
      case File:
      case String:
      case Text:
      case Unknown:
      case UUID:
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
      case DateTime:
        return TSDataType.INT64;
      case Float:
        return TSDataType.FLOAT;
      case Double:
        return TSDataType.DOUBLE;
      case Boolean:
        return TSDataType.BOOLEAN;
      case DataSet:
        // TODO: This is a pretty complex datatype ...
      case Template:
        // TODO: This is a pretty complex datatype ...
    }
    return TSDataType.TEXT;
  }

  protected String getStringValueForMetric(
      SparkplugBProto.Payload.Metric metric, int sparkplugBDataType) {
    SparkplugBProto.DataType dataType = SparkplugBProto.DataType.forNumber(sparkplugBDataType);
    switch (dataType) {
      case Bytes:
        throw new RuntimeException("No idea how to read this");
      case File:
        throw new RuntimeException("No idea how to read this");
      case String:
        return metric.getStringValue();
      case Text:
        return metric.getStringValue();
      case Unknown:
        throw new RuntimeException("No idea how to read this");
      case UUID:
        return metric.getStringValue();
      case Int8:
        return Integer.toString(metric.getIntValue());
      case Int16:
        return Integer.toString(metric.getIntValue());
      case Int32:
        return Integer.toString(metric.getIntValue());
      case UInt8:
        return Integer.toString(metric.getIntValue());
      case UInt16:
        return Integer.toString(metric.getIntValue());
      case Int64:
        return Long.toString(metric.getLongValue());
      case UInt32:
        return Long.toString(metric.getLongValue());
      case UInt64:
        return Long.toString(metric.getLongValue());
      case DateTime:
        return Long.toString(metric.getLongValue());
      case Float:
        return Float.toString(metric.getFloatValue());
      case Double:
        return Double.toString(metric.getDoubleValue());
      case Boolean:
        return Boolean.toString(metric.getBooleanValue());
      case DataSet:
        // TODO: This is a pretty complex datatype ...
        return "To be implemented";
      case Template:
        // TODO: This is a pretty complex datatype ...
        return "To be implemented";
    }
    throw new RuntimeException("No idea how to read this");
  }
}
