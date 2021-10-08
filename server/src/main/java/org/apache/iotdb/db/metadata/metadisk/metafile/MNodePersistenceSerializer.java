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
package org.apache.iotdb.db.metadata.metadisk.metafile;

import org.apache.iotdb.db.metadata.mnode.*;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MNodePersistenceSerializer implements IMNodeSerializer {

  Map<String, Template> templateMap = new ConcurrentHashMap<>();

  @Override
  public ByteBuffer serializeMNode(IMNode mNode) {
    ByteBuffer dataBuffer = ByteBuffer.allocate(evaluateMNodeLength(mNode));
    serializeMNode(mNode, dataBuffer);
    return dataBuffer;
  }

  @Override
  public void serializeMNode(IMNode mNode, ByteBuffer dataBuffer) {
    if (mNode.isStorageGroup()) {
      serializeStorageGroupMNode((StorageGroupMNode) mNode, dataBuffer);
    } else if (mNode.isMeasurement()) {
      serializeMeasurementMNode((MeasurementMNode) mNode, dataBuffer);
    } else {
      serializeInternalMNode((InternalMNode) mNode, dataBuffer);
    }
  }

  @Override
  public IMNode deserializeMNode(ByteBuffer dataBuffer, int type) {
    switch (type) {
      case INTERNAL_MNODE:
        return deserializeInternalMNode(dataBuffer);
      case STORAGE_GROUP_MNODE:
        return deserializeStorageGroupMNode(dataBuffer);
      case MEASUREMENT_MNODE:
        return deserializeMeasurementMNode(dataBuffer);
      default:
        return null;
    }
  }

  @Override
  public ByteBuffer serializeInternalMNode(InternalMNode mNode) {
    ByteBuffer dataBuffer = ByteBuffer.allocate(evaluateMNodeLength(mNode));
    serializeInternalMNode(mNode, dataBuffer);
    return dataBuffer;
  }

  @Override
  public void serializeInternalMNode(InternalMNode mNode, ByteBuffer dataBuffer) {
    ReadWriteIOUtils.writeVar(mNode.getName(), dataBuffer);

    ReadWriteIOUtils.write(mNode.isUseTemplate(), dataBuffer);
    Template template = mNode.getDeviceTemplate();
    if (template != null) {
      templateMap.put(template.getName(), template);
    }
    String templateName = template == null ? "" : template.getName();
    ReadWriteIOUtils.writeVar(templateName, dataBuffer);

    serializeChildren(mNode.getChildren(), dataBuffer);
    serializeChildren(mNode.getAliasChildren(), dataBuffer);
    dataBuffer.flip();
  }

  @Override
  public InternalMNode deserializeInternalMNode(ByteBuffer dataBuffer) {
    InternalMNode mNode = new InternalMNode(null, null);
    mNode.setName(ReadWriteIOUtils.readVarIntString(dataBuffer));

    mNode.setUseTemplate(ReadWriteIOUtils.readBool(dataBuffer));
    String templateName = ReadWriteIOUtils.readVarIntString(dataBuffer);
    mNode.setDeviceTemplate(templateMap.get(templateName));

    deserializeChildren(mNode, dataBuffer);
    return mNode;
  }

  @Override
  public ByteBuffer serializeStorageGroupMNode(StorageGroupMNode mNode) {
    ByteBuffer dataBuffer = ByteBuffer.allocate(evaluateMNodeLength(mNode));
    serializeStorageGroupMNode(mNode, dataBuffer);
    return dataBuffer;
  }

  @Override
  public void serializeStorageGroupMNode(StorageGroupMNode mNode, ByteBuffer dataBuffer) {
    ReadWriteIOUtils.writeVar(mNode.getName(), dataBuffer);

    ReadWriteIOUtils.write(mNode.isUseTemplate(), dataBuffer);
    Template template = mNode.getDeviceTemplate();
    if (template != null) {
      templateMap.put(template.getName(), template);
    }
    String templateName = template == null ? "" : template.getName();
    ReadWriteIOUtils.writeVar(templateName, dataBuffer);

    dataBuffer.putLong(mNode.getDataTTL());
    serializeChildren(mNode.getChildren(), dataBuffer);
    serializeChildren(mNode.getAliasChildren(), dataBuffer);
    dataBuffer.flip();
  }

  @Override
  public StorageGroupMNode deserializeStorageGroupMNode(ByteBuffer dataBuffer) {
    StorageGroupMNode mNode = new StorageGroupMNode(null, null, 0);
    mNode.setName(ReadWriteIOUtils.readVarIntString(dataBuffer));

    mNode.setUseTemplate(ReadWriteIOUtils.readBool(dataBuffer));
    String templateName = ReadWriteIOUtils.readVarIntString(dataBuffer);
    mNode.setDeviceTemplate(templateMap.get(templateName));

    mNode.setDataTTL(dataBuffer.getLong());
    deserializeChildren(mNode, dataBuffer);
    return mNode;
  }

  @Override
  public ByteBuffer serializeMeasurementMNode(MeasurementMNode mNode) {
    ByteBuffer dataBuffer = ByteBuffer.allocate(evaluateMNodeLength(mNode));
    serializeMeasurementMNode(mNode, dataBuffer);
    return dataBuffer;
  }

  @Override
  public void serializeMeasurementMNode(MeasurementMNode mNode, ByteBuffer dataBuffer) {
    ReadWriteIOUtils.writeVar(mNode.getName(), dataBuffer);

    ReadWriteIOUtils.write(mNode.isUseTemplate(), dataBuffer);
    Template template = mNode.getDeviceTemplate();
    if (template != null) {
      templateMap.put(template.getName(), template);
    }
    String templateName = template == null ? "" : template.getName();
    ReadWriteIOUtils.writeVar(templateName, dataBuffer);

    ReadWriteIOUtils.writeVar(mNode.getAlias() == null ? "" : mNode.getAlias(), dataBuffer);
    dataBuffer.putLong(mNode.getOffset());
    serializeMeasurementSchema(mNode.getSchema(), dataBuffer);
    serializeLastCache(mNode.getCachedLast(), mNode.getSchema().getType(), dataBuffer);
    serializeChildren(mNode.getChildren(), dataBuffer);
    serializeChildren(mNode.getAliasChildren(), dataBuffer);
    dataBuffer.flip();
  }

  @Override
  public MeasurementMNode deserializeMeasurementMNode(ByteBuffer dataBuffer) {
    MeasurementMNode mNode = new MeasurementMNode(null, null, null, null);
    mNode.setName(ReadWriteIOUtils.readVarIntString(dataBuffer));

    mNode.setUseTemplate(ReadWriteIOUtils.readBool(dataBuffer));
    String templateName = ReadWriteIOUtils.readVarIntString(dataBuffer);
    mNode.setDeviceTemplate(templateMap.get(templateName));

    String alias = ReadWriteIOUtils.readVarIntString(dataBuffer);
    mNode.setAlias("".equals(alias) ? null : alias);
    mNode.setOffset(dataBuffer.getLong());
    deserializeMeasurementSchema(mNode, dataBuffer);
    deserializeLastCache(mNode, dataBuffer);
    deserializeChildren(mNode, dataBuffer);
    return mNode;
  }

  private void serializeChildren(Map<String, IMNode> children, ByteBuffer dataBuffer) {
    for (String childName : children.keySet()) {
      ReadWriteIOUtils.writeVar(childName, dataBuffer);
      dataBuffer.putLong(children.get(childName).getPersistenceInfo().getStartPosition());
    }
    dataBuffer.put((byte) 0);
  }

  private void deserializeChildren(IMNode mNode, ByteBuffer byteBuffer) {
    String name;
    name = ReadWriteIOUtils.readVarIntString(byteBuffer);
    Map<Long, String> children = new HashMap<>();
    while (name != null && !name.equals("")) {
      children.put(byteBuffer.getLong(), name);
      name = ReadWriteIOUtils.readVarIntString(byteBuffer);
    }
    Map<Long, String> aliasChildren = new HashMap<>();
    name = ReadWriteIOUtils.readVarIntString(byteBuffer);
    while (name != null && !name.equals("")) {
      aliasChildren.put(byteBuffer.getLong(), name);
      name = ReadWriteIOUtils.readVarIntString(byteBuffer);
    }

    IMNode child;
    for (long position : children.keySet()) {
      child = new PersistenceMNode(position);
      mNode.addChild(children.get(position), child);
      if (aliasChildren.containsKey(position)) {
        mNode.addAlias(aliasChildren.get(position), child);
      }
    }
  }

  private void serializeMeasurementSchema(MeasurementSchema schema, ByteBuffer dataBuffer) {
    dataBuffer.put(schema.getType().serialize());
    dataBuffer.put(schema.getEncodingType().serialize());
    dataBuffer.put(schema.getCompressor().serialize());
    if (schema.getProps() != null) {
      for (Map.Entry<String, String> entry : schema.getProps().entrySet()) {
        ReadWriteIOUtils.writeVar(entry.getKey(), dataBuffer);
        ReadWriteIOUtils.writeVar(entry.getValue(), dataBuffer);
      }
    }
    dataBuffer.put((byte) 0);
  }

  private void deserializeMeasurementSchema(MeasurementMNode mNode, ByteBuffer dataBuffer) {
    byte type = dataBuffer.get();
    byte encoding = dataBuffer.get();
    byte compressor = dataBuffer.get();
    Map<String, String> props = new HashMap<>();
    String key;
    key = ReadWriteIOUtils.readVarIntString(dataBuffer);
    while (key != null && !key.equals("")) {
      props.put(key, ReadWriteIOUtils.readVarIntString(dataBuffer));
      key = ReadWriteIOUtils.readVarIntString(dataBuffer);
    }
    MeasurementSchema schema =
        new MeasurementSchema(
            mNode.getName(),
            TSDataType.deserialize(type),
            TSEncoding.deserialize(encoding),
            CompressionType.deserialize(compressor),
            props.size() == 0 ? null : props);
    mNode.setSchema(schema);
  }

  private void serializeLastCache(
      TimeValuePair cachedLastValuePair, TSDataType dataType, ByteBuffer dataBuffer) {
    if (cachedLastValuePair != null) {
      dataBuffer.putLong(cachedLastValuePair.getTimestamp());
      TsPrimitiveType value = cachedLastValuePair.getValue();
      switch (dataType) {
        case BOOLEAN:
          dataBuffer.put((byte) (value.getBoolean() ? 1 : 0));
          break;
        case INT32:
          dataBuffer.putInt(value.getInt());
          break;
        case INT64:
          dataBuffer.putLong(value.getLong());
          break;
        case FLOAT:
          dataBuffer.putFloat(value.getFloat());
          break;
        case DOUBLE:
          dataBuffer.putDouble(value.getDouble());
          break;
        case TEXT:
          dataBuffer.putInt(value.getBinary().getLength());
          dataBuffer.put(value.getBinary().getValues());
          break;
        default:
          throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
      }
    } else {
      dataBuffer.putLong(-1);
    }
  }

  private void deserializeLastCache(MeasurementMNode mNode, ByteBuffer dataBuffer) {
    long timestamp = dataBuffer.getLong();
    if (timestamp != -1) {
      TSDataType dataType = mNode.getSchema().getType();
      TsPrimitiveType value;
      switch (dataType) {
        case BOOLEAN:
          value = TsPrimitiveType.getByType(dataType, dataBuffer.get() == 1);
          break;
        case INT32:
          value = TsPrimitiveType.getByType(dataType, dataBuffer.getInt());
          break;
        case INT64:
          value = TsPrimitiveType.getByType(dataType, dataBuffer.getLong());
          break;
        case FLOAT:
          value = TsPrimitiveType.getByType(dataType, dataBuffer.getFloat());
          break;
        case DOUBLE:
          value = TsPrimitiveType.getByType(dataType, dataBuffer.getDouble());
          break;
        case TEXT:
          byte[] content = new byte[dataBuffer.getInt()];
          dataBuffer.get(content);
          value = TsPrimitiveType.getByType(dataType, new Binary(content));
          break;
        default:
          throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
      }
      TimeValuePair lastCache = new TimeValuePair(timestamp, value);
      mNode.updateCachedLast(lastCache, false, 0L);
    }
  }

  @Override
  public int evaluateMNodeLength(IMNode mNode) {
    int length = 0;
    length +=
        ReadWriteForEncodingUtils.varIntSize(mNode.getName().length())
            + mNode.getName().length(); // string.length()==string.getBytes().length

    // template
    length += 1;
    Template template = mNode.getDeviceTemplate();
    String templateName = template == null ? "" : template.getName();
    length += ReadWriteForEncodingUtils.varIntSize(templateName.length());
    length += templateName.length();

    // children
    length += evaluateChildrenLength(mNode.getChildren());
    // alias children
    length += evaluateChildrenLength(mNode.getAliasChildren());

    if (mNode.isStorageGroup()) {
      length += 8; // TTL
    }

    if (mNode.isMeasurement()) {
      length += evaluateMeasurementDataLength((MeasurementMNode) mNode);
    }
    return length;
  }

  private int evaluateChildrenLength(Map<String, IMNode> children) {
    int length = 0;
    for (String childName : children.keySet()) {
      length +=
          ReadWriteForEncodingUtils.varIntSize(childName.length())
              + childName.length()
              + 8; // child name and child position
    }
    length += 1; // children end tag
    return length;
  }

  private int evaluateMeasurementDataLength(MeasurementMNode measurementMNode) {
    int length = 0;
    String alias = measurementMNode.getAlias(); // alias
    if (alias == null) {
      length += ReadWriteForEncodingUtils.varIntSize(0);
    } else {
      length += ReadWriteForEncodingUtils.varIntSize(alias.length()) + alias.length();
    }
    length += 8; // offset
    length += evaluateMeasurementSchemaLength(measurementMNode.getSchema()); // schema
    length +=
        evaluateLastCacheLength(
            measurementMNode.getCachedLast(), measurementMNode.getSchema().getType()); // lastCache

    return length;
  }

  private int evaluateMeasurementSchemaLength(MeasurementSchema schema) {
    int length = 3; // type, encoding, compressor
    if (schema.getProps() != null) {
      for (Map.Entry<String, String> entry : schema.getProps().entrySet()) {
        length +=
            ReadWriteForEncodingUtils.varIntSize(entry.getKey().length()) + entry.getKey().length();
        length +=
            ReadWriteForEncodingUtils.varIntSize(entry.getValue().length())
                + entry.getValue().length();
      }
    }
    length += 1; // end tag of props
    return length;
  }

  private int evaluateLastCacheLength(TimeValuePair cachedLastValuePair, TSDataType dataType) {
    int length = 8; // timestamp, -1 means lastCache is null
    if (cachedLastValuePair != null) {
      switch (dataType) { // value
        case BOOLEAN:
          length += 1;
          break;
        case INT32:
          length += 4;
          break;
        case INT64:
          length += 8;
          break;
        case FLOAT:
          length += 4;
          break;
        case DOUBLE:
          length += 8;
          break;
        case TEXT:
          length += 4 + cachedLastValuePair.getValue().getBinary().getLength(); // length + data
          break;
        default:
          throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
      }
    }
    return length;
  }
}
