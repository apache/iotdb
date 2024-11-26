/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.compatibility.DeserializeConfig;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/** TSFileMetaData collects all metadata info and saves in its data structure. */
public class TsFileMetadata {

  // bloom filter
  private BloomFilter bloomFilter;

  // List of <name, offset, childMetadataIndexType>
  private Map<String, MetadataIndexNode> tableMetadataIndexNodeMap;
  private Map<String, TableSchema> tableSchemaMap;
  private Map<String, String> tsFileProperties;

  // offset of MetaMarker.SEPARATOR
  private long metaOffset;
  // offset from MetaMarker.SEPARATOR (exclusive) to tsFileProperties
  private int propertiesOffset;

  private byte[] dataEncryptKey;

  private String encryptType;

  /**
   * deserialize data from the buffer.
   *
   * @param buffer -buffer use to deserialize
   * @return -an instance of TsFileMetaData
   */
  public static TsFileMetadata deserializeFrom(ByteBuffer buffer, DeserializeConfig context) {
    TsFileMetadata fileMetaData = new TsFileMetadata();

    int startPos = buffer.position();
    // metadataIndex
    int tableIndexNodeNum = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    Map<String, MetadataIndexNode> tableIndexNodeMap = new TreeMap<>();
    for (int i = 0; i < tableIndexNodeNum; i++) {
      String tableName = ReadWriteIOUtils.readVarIntString(buffer);
      MetadataIndexNode metadataIndexNode =
          context.deviceMetadataIndexNodeBufferDeserializer.deserialize(buffer, context);
      tableIndexNodeMap.put(tableName, metadataIndexNode);
    }
    fileMetaData.setTableMetadataIndexNodeMap(tableIndexNodeMap);

    // tableSchemas
    int tableSchemaNum = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    Map<String, TableSchema> tableSchemaMap = new HashMap<>();
    for (int i = 0; i < tableSchemaNum; i++) {
      String tableName = ReadWriteIOUtils.readVarIntString(buffer);
      TableSchema tableSchema = context.tableSchemaBufferDeserializer.deserialize(buffer, context);
      tableSchema.setTableName(tableName);
      tableSchemaMap.put(tableName, tableSchema);
    }
    fileMetaData.setTableSchemaMap(tableSchemaMap);

    // metaOffset
    long metaOffset = ReadWriteIOUtils.readLong(buffer);
    fileMetaData.setMetaOffset(metaOffset);

    // read bloom filter
    if (buffer.hasRemaining()) {
      byte[] bytes = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);
      if (bytes.length != 0) {
        int filterSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        int hashFunctionSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        fileMetaData.bloomFilter =
            BloomFilter.buildBloomFilter(bytes, filterSize, hashFunctionSize);
      }
    }

    fileMetaData.propertiesOffset = buffer.position() - startPos;

    if (buffer.hasRemaining()) {
      int propertiesSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      Map<String, String> propertiesMap = new HashMap<>();
      for (int i = 0; i < propertiesSize; i++) {
        String key = ReadWriteIOUtils.readVarIntString(buffer);
        String value = ReadWriteIOUtils.readVarIntString(buffer);
        propertiesMap.put(key, value);
      }
      // if the file is not encrypted, set the default value(for compatible reason)
      if (!propertiesMap.containsKey("encryptLevel") || propertiesMap.get("encryptLevel") == null) {
        propertiesMap.put("encryptLevel", "0");
        propertiesMap.put("encryptType", "org.apache.tsfile.encrypt.UNENCRYPTED");
        propertiesMap.put("encryptKey", "");
      } else if (propertiesMap.get("encryptLevel").equals("0")) {
        propertiesMap.put("encryptType", "org.apache.tsfile.encrypt.UNENCRYPTED");
        propertiesMap.put("encryptKey", "");
      } else if (propertiesMap.get("encryptLevel").equals("1")) {
        if (!propertiesMap.containsKey("encryptType")) {
          throw new EncryptException("TsfileMetadata lack of encryptType while encryptLevel is 1");
        }
        if (!propertiesMap.containsKey("encryptKey")) {
          throw new EncryptException("TsfileMetadata lack of encryptKey while encryptLevel is 1");
        }
        if (propertiesMap.get("encryptKey") == null || propertiesMap.get("encryptKey").isEmpty()) {
          throw new EncryptException("TsfileMetadata null encryptKey while encryptLevel is 1");
        }
        String str = propertiesMap.get("encryptKey");
        fileMetaData.dataEncryptKey = EncryptUtils.getSecondKeyFromStr(str);
        fileMetaData.encryptType = propertiesMap.get("encryptType");
      } else if (propertiesMap.get("encryptLevel").equals("2")) {
        if (!propertiesMap.containsKey("encryptType")) {
          throw new EncryptException("TsfileMetadata lack of encryptType while encryptLevel is 2");
        }
        if (!propertiesMap.containsKey("encryptKey")) {
          throw new EncryptException("TsfileMetadata lack of encryptKey while encryptLevel is 2");
        }
        if (propertiesMap.get("encryptKey") == null || propertiesMap.get("encryptKey").isEmpty()) {
          throw new EncryptException("TsfileMetadata null encryptKey while encryptLevel is 2");
        }
        IDecryptor decryptor =
            IDecryptor.getDecryptor(
                TSFileDescriptor.getInstance().getConfig().getEncryptType(),
                TSFileDescriptor.getInstance().getConfig().getEncryptKey().getBytes());
        String str = propertiesMap.get("encryptKey");
        fileMetaData.dataEncryptKey = decryptor.decrypt(EncryptUtils.getSecondKeyFromStr(str));
        fileMetaData.encryptType = propertiesMap.get("encryptType");
      } else {
        throw new EncryptException(
            "Unsupported encryptLevel: " + propertiesMap.get("encryptLevel"));
      }
      fileMetaData.tsFileProperties = propertiesMap;
    }

    return fileMetaData;
  }

  public EncryptParameter getEncryptParam() {
    if (dataEncryptKey == null) {
      return new EncryptParameter("org.apache.tsfile.encrypt.UNENCRYPTED", null);
    }
    return new EncryptParameter(encryptType, dataEncryptKey);
  }

  public void addProperty(String key, String value) {
    if (tsFileProperties == null) {
      tsFileProperties = new HashMap<>();
    }
    tsFileProperties.put(key, value);
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  public void setBloomFilter(BloomFilter bloomFilter) {
    this.bloomFilter = bloomFilter;
  }

  /**
   * use the given outputStream to serialize.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   * @throws IOException error when operating outputStream
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    if (tableMetadataIndexNodeMap != null) {
      byteLen +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(
              tableMetadataIndexNodeMap.size(), outputStream);
      for (Entry<String, MetadataIndexNode> entry : tableMetadataIndexNodeMap.entrySet()) {
        byteLen += ReadWriteIOUtils.writeVar(entry.getKey(), outputStream);
        byteLen += entry.getValue().serializeTo(outputStream);
      }
    } else {
      byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(0, outputStream);
    }

    if (tableSchemaMap != null) {
      byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(tableSchemaMap.size(), outputStream);
      for (Entry<String, TableSchema> entry : tableSchemaMap.entrySet()) {
        byteLen += ReadWriteIOUtils.writeVar(entry.getKey(), outputStream);
        byteLen += entry.getValue().serialize(outputStream);
      }
    } else {
      byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(0, outputStream);
    }

    // metaOffset
    byteLen += ReadWriteIOUtils.write(metaOffset, outputStream);
    if (bloomFilter != null) {
      byteLen += serializeBloomFilter(outputStream, bloomFilter);
    } else {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    }

    byteLen +=
        ReadWriteForEncodingUtils.writeVarInt(
            tsFileProperties != null ? tsFileProperties.size() : 0, outputStream);
    if (tsFileProperties != null) {
      for (Entry<String, String> entry : tsFileProperties.entrySet()) {
        byteLen += ReadWriteIOUtils.writeVar(entry.getKey(), outputStream);
        byteLen += ReadWriteIOUtils.writeVar(entry.getValue(), outputStream);
      }
    }

    return byteLen;
  }

  public int serializeBloomFilter(OutputStream outputStream, BloomFilter filter)
      throws IOException {
    int byteLen = 0;
    byte[] bytes = filter.serialize();
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(bytes.length, outputStream);
    if (bytes.length > 0) {
      outputStream.write(bytes);
      byteLen += bytes.length;
      byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(filter.getSize(), outputStream);
      byteLen +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(filter.getHashFunctionSize(), outputStream);
    }
    return byteLen;
  }

  public long getMetaOffset() {
    return metaOffset;
  }

  public void setMetaOffset(long metaOffset) {
    this.metaOffset = metaOffset;
  }

  public void setTableMetadataIndexNodeMap(
      Map<String, MetadataIndexNode> tableMetadataIndexNodeMap) {
    this.tableMetadataIndexNodeMap = tableMetadataIndexNodeMap;
  }

  public void setTableSchemaMap(Map<String, TableSchema> tableSchemaMap) {
    this.tableSchemaMap = tableSchemaMap;
  }

  public Map<String, MetadataIndexNode> getTableMetadataIndexNodeMap() {
    return tableMetadataIndexNodeMap;
  }

  public MetadataIndexNode getTableMetadataIndexNode(String tableName) {
    MetadataIndexNode metadataIndexNode = tableMetadataIndexNodeMap.get(tableName);
    if (metadataIndexNode == null) {
      metadataIndexNode = tableMetadataIndexNodeMap.get("");
    }
    return metadataIndexNode;
  }

  public Map<String, TableSchema> getTableSchemaMap() {
    return tableSchemaMap;
  }
}
