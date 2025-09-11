package org.apache.iotdb.mqtt.server;

import org.apache.iotdb.db.protocol.mqtt.Message;
import org.apache.iotdb.db.protocol.mqtt.PayloadFormatter;
import org.apache.iotdb.db.protocol.mqtt.TableMessage;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tsfile.enums.TSDataType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CustomizedLinePayloadFormatter implements PayloadFormatter {

  @Override
  public List<Message> format(String topic, ByteBuf payload) {
    // Suppose the payload is a line format
    if (payload == null) {
      return null;
    }

    String line = payload.toString(StandardCharsets.UTF_8);
    // parse data from the line and generate Messages and put them into List<Meesage> ret
    List<Message> ret = new ArrayList<>();
    // this is just an example, so we just generate some Messages directly
    for (int i = 0; i < 3; i++) {
      long ts = i;
      TableMessage message = new TableMessage();

      // Parsing Database Name
      message.setDatabase("db" + i);

      // Parsing Table Names
      message.setTable("t" + i);

      // Parsing Tags
      List<String> tagKeys = new ArrayList<>();
      tagKeys.add("tag1" + i);
      tagKeys.add("tag2" + i);
      List<Object> tagValues = new ArrayList<>();
      tagValues.add("t_value1" + i);
      tagValues.add("t_value2" + i);
      message.setTagKeys(tagKeys);
      message.setTagValues(tagValues);

      // Parsing Attributes
      List<String> attributeKeys = new ArrayList<>();
      List<Object> attributeValues = new ArrayList<>();
      attributeKeys.add("attr1" + i);
      attributeKeys.add("attr2" + i);
      attributeValues.add("a_value1" + i);
      attributeValues.add("a_value2" + i);
      message.setAttributeKeys(attributeKeys);
      message.setAttributeValues(attributeValues);

      // Parsing Fields
      List<String> fields = Arrays.asList("field1" + i, "field2" + i);
      List<TSDataType> dataTypes = Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT);
      List<Object> values = Arrays.asList("4.0" + i, "5.0" + i);
      message.setFields(fields);
      message.setDataTypes(dataTypes);
      message.setValues(values);

      //// Parsing timestamp
      message.setTimestamp(ts);
      ret.add(message);
    }
    return ret;
  }

  @Override
  @Deprecated
  public List<Message> format(ByteBuf payload) {
    throw new NotImplementedException();
  }

  @Override
  public String getName() {
    // set the value of mqtt_payload_formatter in iotdb-system.properties as the following string:
    return "CustomizedLine";
  }

  @Override
  public String getType() {
    return PayloadFormatter.TABLE_TYPE;
  }
}
