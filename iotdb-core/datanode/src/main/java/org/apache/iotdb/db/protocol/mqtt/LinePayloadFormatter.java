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

import io.netty.buffer.ByteBuf;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.NotImplementedException;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The Line payload formatter. myTable,tag1=value1,tag2=value2 attr1=value1,attr2=value2
 * fieldKey="fieldValue" 1740109006000 \n myTable,tag1=value1,tag2=value2 fieldKey="fieldValue"
 * 1740109006000
 */
public class LinePayloadFormatter implements PayloadFormatter {

  private static final Logger log = LoggerFactory.getLogger(LinePayloadFormatter.class);

  /*
  Regular expression matching line protocol ，the attributes field is not required
  */
  private static final String REGEX =
      "(?<table>\\w+)(,(?<tags>[^ ]+))?(\\s+(?<attributes>[^ ]+))?\\s+(?<fields>[^ ]+)\\s+(?<timestamp>\\d+)";
  private static final String COMMA = ",";
  private static final String WELL = "#";
  private static final String LINE_BREAK = "\n";
  private static final String EQUAL = "=";
  private static final String TABLE = "table";
  private static final String TAGS = "tags";
  private static final String ATTRIBUTES = "attributes";
  private static final String FIELDS = "fields";
  private static final String TIMESTAMP = "timestamp";
  private static final String NULL = "null";

  private final Pattern pattern;

  public LinePayloadFormatter() {
    pattern = Pattern.compile(REGEX);
  }

  @Override
  public List<Message> format(String topic, ByteBuf payload) {
    List<Message> messages = new ArrayList<>();
    if (payload == null) {
      return messages;
    }

    String txt = payload.toString(StandardCharsets.UTF_8);
    String[] lines = txt.split(LINE_BREAK);
    // '/' previously defined as a database name
    String database = !topic.contains("/") ? topic : topic.substring(0, topic.indexOf("/"));
    for (String line : lines) {
      if (line.trim().startsWith(WELL)) {
        continue;
      }
      TableMessage message = new TableMessage();
      try {
        Matcher matcher = pattern.matcher(line.trim());
        if (!matcher.matches()) {
          log.warn("Invalid line protocol format ,line is {}", line);
          continue;
        }

        // Parsing Database Name
        message.setDatabase((database));

        // Parsing Table Names
        message.setTable(matcher.group(TABLE));

        // Parsing Tags
        if (!setTags(matcher, message)) {
          log.warn("The tags is error , line is {}", line);
          continue;
        }

        // Parsing Attributes
        if (!setAttributes(matcher, message)) {
          log.warn("The attributes is error , line is {}", line);
          continue;
        }

        // Parsing Fields
        if (!setFields(matcher, message)) {
          log.warn("The fields is error , line is {}", line);
          continue;
        }

        // Parsing timestamp
        if (!setTimestamp(matcher, message)) {
          log.warn("The timestamp is error , line is {}", line);
          continue;
        }

        messages.add(message);
      } catch (Exception e) {
        log.warn(
            "The line pattern parsing fails, and the failed line message is {} ,exception is",
            line,
            e);
      }
    }
    return messages;
  }

  @Override
  @Deprecated
  public List<Message> format(ByteBuf payload) {
    throw new NotImplementedException();
  }

  private boolean setTags(Matcher matcher, TableMessage message) {
    List<String> tagKeys = new ArrayList<>();
    List<Object> tagValues = new ArrayList<>();
    String tagsGroup = matcher.group(TAGS);
    if (tagsGroup != null && !tagsGroup.isEmpty()) {
      String[] tagPairs = tagsGroup.split(COMMA);
      for (String tagPair : tagPairs) {
        if (!tagPair.isEmpty()) {
          String[] keyValue = tagPair.split(EQUAL);
          if (keyValue.length == 2 && !NULL.equals(keyValue[1])) {
            tagKeys.add(keyValue[0]);
            tagValues.add(new Binary[] {new Binary(keyValue[1].getBytes(StandardCharsets.UTF_8))});
          }
        }
      }
    }
    if (!tagKeys.isEmpty() && !tagValues.isEmpty() && tagKeys.size() == tagValues.size()) {
      message.setTagKeys(tagKeys);
      message.setTagValues(tagValues);
      return true;
    } else {
      return false;
    }
  }

  private boolean setAttributes(Matcher matcher, TableMessage message) {
    List<String> attributeKeys = new ArrayList<>();
    List<Object> attributeValues = new ArrayList<>();
    String attributesGroup = matcher.group(ATTRIBUTES);
    if (attributesGroup != null && !attributesGroup.isEmpty()) {
      String[] attributePairs = attributesGroup.split(COMMA);
      for (String attributePair : attributePairs) {
        if (!attributePair.isEmpty()) {
          String[] keyValue = attributePair.split(EQUAL);
          if (keyValue.length == 2 && !NULL.equals(keyValue[1])) {
            attributeKeys.add(keyValue[0]);
            attributeValues.add(
                new Binary[] {new Binary(keyValue[1].getBytes(StandardCharsets.UTF_8))});
          }
        }
      }
    }
    if (attributeKeys.size() == attributeValues.size()) {
      message.setAttributeKeys(attributeKeys);
      message.setAttributeValues(attributeValues);
      return true;
    } else {
      return false;
    }
  }

  private boolean setFields(Matcher matcher, TableMessage message) {
    List<String> fields = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    String fieldsGroup = matcher.group(FIELDS);
    if (fieldsGroup != null && !fieldsGroup.isEmpty()) {
      String[] fieldPairs = splitFieldPairs(fieldsGroup);
      for (String fieldPair : fieldPairs) {
        if (!fieldPair.isEmpty()) {
          String[] keyValue = fieldPair.split(EQUAL);
          if (keyValue.length == 2 && !NULL.equals(keyValue[1])) {
            fields.add(keyValue[0]);
            Pair<TSDataType, Object> typeAndValue = analyticValue(keyValue[1]);
            values.add(typeAndValue.getRight());
            dataTypes.add(typeAndValue.getLeft());
          }
        }
      }
    }
    if (!fields.isEmpty() && !values.isEmpty() && fields.size() == values.size()) {
      message.setFields(fields);
      message.setDataTypes(dataTypes);
      message.setValues(values);
      return true;
    } else {
      return false;
    }
  }

  private String[] splitFieldPairs(String fieldsGroup) {

    if (fieldsGroup == null || fieldsGroup.isEmpty()) return new String[0];
    Matcher m = Pattern.compile("\\w+=\"[^\"]*\"|\\w+=[^,]*").matcher(fieldsGroup);
    Stream.Builder<String> builder = Stream.builder();

    while (m.find()) builder.add(m.group());
    return builder.build().toArray(String[]::new);
  }

  private Pair<TSDataType, Object> analyticValue(String value) {
    if (value.startsWith("\"") && value.endsWith("\"")) {
      // String
      return new Pair<>(
          TSDataType.TEXT,
          new Binary[] {
            new Binary(value.substring(1, value.length() - 1).getBytes(StandardCharsets.UTF_8))
          });
    } else if (value.equalsIgnoreCase("t")
        || value.equalsIgnoreCase("true")
        || value.equalsIgnoreCase("f")
        || value.equalsIgnoreCase("false")) {
      // boolean
      return new Pair<>(
          TSDataType.BOOLEAN,
          new boolean[] {value.equalsIgnoreCase("t") || value.equalsIgnoreCase("true")});
    } else if (value.endsWith("f")) {
      // float
      return new Pair<>(
          TSDataType.FLOAT, new float[] {Float.parseFloat(value.substring(0, value.length() - 1))});
    } else if (value.endsWith("i32")) {
      // int
      return new Pair<>(
          TSDataType.INT32, new int[] {Integer.parseInt(value.substring(0, value.length() - 3))});
    } else if (value.endsWith("u") || value.endsWith("i")) {
      // long
      return new Pair<>(
          TSDataType.INT64, new long[] {Long.parseLong(value.substring(0, value.length() - 1))});
    } else {
      // double
      return new Pair<>(TSDataType.DOUBLE, new double[] {Double.parseDouble(value)});
    }
  }

  private boolean setTimestamp(Matcher matcher, TableMessage message) {
    String timestampGroup = matcher.group(TIMESTAMP);
    if (timestampGroup != null && !timestampGroup.isEmpty()) {
      message.setTimestamp(Long.parseLong(timestampGroup));
      return true;
    } else {
      return false;
    }
  }

  @Override
  public String getName() {
    return "line";
  }

  @Override
  public String getType() {
    return PayloadFormatter.TABLE_TYPE;
  }
}
