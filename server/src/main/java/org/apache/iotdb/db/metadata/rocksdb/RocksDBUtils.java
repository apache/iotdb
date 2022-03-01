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
package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.ArrayUtils;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DATA_BLOCK_TYPE_ALIAS;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DATA_BLOCK_TYPE_ATTRIBUTES;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DATA_BLOCK_TYPE_ORIGIN_KEY;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DATA_BLOCK_TYPE_SCHEMA;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DATA_BLOCK_TYPE_TAGS;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DATA_BLOCK_TYPE_TTL;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DATA_VERSION;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.DEFAULT_FLAG;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.ESCAPE_PATH_SEPARATOR;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.FLAG_HAS_ALIAS;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.FLAG_HAS_ATTRIBUTES;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.FLAG_HAS_TAGS;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.FLAG_SET_TTL;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_ALIAS;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_ENTITY;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_INTERNAL;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_MEASUREMENT;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.NODE_TYPE_SG;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.PATH_SEPARATOR;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.ROOT;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.ROOT_CHAR;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.ROOT_STRING;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.ZERO;

public class RocksDBUtils {

  private static final Logger logger = LoggerFactory.getLogger(RocksDBUtils.class);

  protected static RocksDBMNodeType[] NODE_TYPE_ARRAY = new RocksDBMNodeType[NODE_TYPE_ALIAS + 1];

  static {
    NODE_TYPE_ARRAY[NODE_TYPE_INTERNAL] = RocksDBMNodeType.INTERNAL;
    NODE_TYPE_ARRAY[NODE_TYPE_SG] = RocksDBMNodeType.STORAGE_GROUP;
    NODE_TYPE_ARRAY[NODE_TYPE_ENTITY] = RocksDBMNodeType.ENTITY;
    NODE_TYPE_ARRAY[NODE_TYPE_MEASUREMENT] = RocksDBMNodeType.MEASUREMENT;
    NODE_TYPE_ARRAY[NODE_TYPE_ALIAS] = RocksDBMNodeType.ALISA;
  }

  protected static byte[] constructDataBlock(byte type, String data) {
    byte[] dataInBytes = data.getBytes();
    int size = dataInBytes.length;
    return Bytes.concat(new byte[] {type}, BytesUtils.intToBytes(size), dataInBytes);
  }

  protected static byte[] toInternalNodeKey(String levelPath) {
    return toRocksDBKey(levelPath, NODE_TYPE_INTERNAL);
  }

  protected static byte[] toStorageNodeKey(String levelPath) {
    return toRocksDBKey(levelPath, NODE_TYPE_SG);
  }

  protected static byte[] toEntityNodeKey(String levelPath) {
    return toRocksDBKey(levelPath, NODE_TYPE_ENTITY);
  }

  protected static byte[] toMeasurementNodeKey(String levelPath) {
    return toRocksDBKey(levelPath, NODE_TYPE_MEASUREMENT);
  }

  protected static byte[] toAliasNodeKey(String levelPath) {
    return toRocksDBKey(levelPath, NODE_TYPE_ALIAS);
  }

  protected static byte[] toRocksDBKey(String levelPath, char type) {
    return (type + levelPath).getBytes();
  }

  public static String getLevelPathPrefix(String[] nodes, int end, int level) {
    StringBuilder builder = new StringBuilder();
    char depth = (char) (ZERO + level);
    builder.append(ROOT).append(PATH_SEPARATOR).append(depth);
    for (int i = 1; i <= end; i++) {
      builder.append(nodes[i]).append(PATH_SEPARATOR).append(depth);
    }
    return builder.toString();
  }

  public static String getLevelPath(String[] nodes, int end) {
    return getLevelPath(nodes, end, end);
  }

  public static String getLevelPath(String[] nodes, int end, int level) {
    StringBuilder builder = new StringBuilder();
    builder.append(ROOT);
    char depth = (char) (ZERO + level);
    for (int i = 1; i <= end; i++) {
      builder.append(PATH_SEPARATOR).append(depth).append(nodes[i]);
    }
    return builder.toString();
  }

  public static String getMeasurementLevelPath(String[] prefixPath, String measurement) {
    String[] nodes = ArrayUtils.add(prefixPath, measurement);
    return getLevelPath(nodes, nodes.length - 1);
  }

  public static List<PartialPath> convertToPartialPath(Collection<String> paths, int level) {
    return paths
        .parallelStream()
        .map(x -> getPartialPathFromInnerPath(x, level))
        .collect(Collectors.toList());
  }

  public static String getNextLevelOfPath(String innerPath, int currentLevel) {
    char levelChar = (char) (ZERO + currentLevel);
    String old = PATH_SEPARATOR + levelChar;
    String target = PATH_SEPARATOR + (char) (levelChar + 1);
    return innerPath.replace(old, target);
  }

  public static String getNextLevelOfPath(String innerPath, char currentLevel) {
    String old = PATH_SEPARATOR + currentLevel;
    String target = PATH_SEPARATOR + (char) (currentLevel + 1);
    return innerPath.replace(old, target);
  }

  public static PartialPath getPartialPathFromInnerPath(String path, int level) {
    char charLevel = (char) (ZERO + level);
    return getPartialPathFromInnerPath(path, charLevel);
  }

  public static PartialPath getPartialPathFromInnerPath(String path, char level) {
    String pathWithoutLevel = path.replace(PATH_SEPARATOR + level, PATH_SEPARATOR);
    String[] nodes = pathWithoutLevel.split(ESCAPE_PATH_SEPARATOR);
    nodes[0] = PATH_ROOT;
    return new PartialPath(nodes);
  }

  public static byte[] buildMeasurementNodeValue(
      IMeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(DATA_VERSION, outputStream);

    byte flag = DEFAULT_FLAG;
    if (alias != null) {
      flag = (byte) (flag | FLAG_HAS_ALIAS);
    }

    if (tags != null && tags.size() > 0) {
      flag = (byte) (flag | FLAG_HAS_TAGS);
    }

    if (attributes != null && attributes.size() > 0) {
      flag = (byte) (flag | FLAG_HAS_ATTRIBUTES);
    }

    ReadWriteIOUtils.write(flag, outputStream);

    if (schema != null) {
      ReadWriteIOUtils.write(DATA_BLOCK_TYPE_SCHEMA, outputStream);
      schema.serializeTo(outputStream);
    }

    if (alias != null) {
      ReadWriteIOUtils.write(DATA_BLOCK_TYPE_ALIAS, outputStream);
      ReadWriteIOUtils.write(alias, outputStream);
    }

    if (tags != null && tags.size() > 0) {
      ReadWriteIOUtils.write(DATA_BLOCK_TYPE_TAGS, outputStream);
      ReadWriteIOUtils.write(tags, outputStream);
    }

    if (attributes != null && attributes.size() > 0) {
      ReadWriteIOUtils.write(DATA_BLOCK_TYPE_TAGS, outputStream);
      ReadWriteIOUtils.write(tags, outputStream);
    }
    return outputStream.toByteArray();
  }

  public static byte[] buildAliasNodeValue(byte[] originKey) {
    byte[] prefix = new byte[] {DATA_VERSION, DEFAULT_FLAG, DATA_BLOCK_TYPE_ORIGIN_KEY};
    byte[] len = BytesUtils.intToBytes(originKey.length);
    return BytesUtils.concatByteArray(BytesUtils.concatByteArray(prefix, len), originKey);
  }

  public static byte[] readOriginKey(ByteBuffer buffer) {
    int len = ReadWriteIOUtils.readInt(buffer);
    return ReadWriteIOUtils.readBytes(buffer, len);
  }

  public static int indexOfDataBlockType(byte[] data, byte type) {
    if ((data[1] & FLAG_SET_TTL) == 0) {
      return -1;
    }

    int index = -1;
    boolean typeExist = false;
    ByteBuffer byteBuffer = ByteBuffer.wrap(data);
    // skip the version flag and node type flag
    ReadWriteIOUtils.readBytes(byteBuffer, 2);
    while (byteBuffer.hasRemaining()) {
      byte blockType = ReadWriteIOUtils.readByte(byteBuffer);
      index = byteBuffer.position();
      switch (blockType) {
        case DATA_BLOCK_TYPE_TTL:
          ReadWriteIOUtils.readLong(byteBuffer);
          break;
        case DATA_BLOCK_TYPE_ALIAS:
          ReadWriteIOUtils.readString(byteBuffer);
          break;
        case DATA_BLOCK_TYPE_ORIGIN_KEY:
          readOriginKey(byteBuffer);
          break;
        case DATA_BLOCK_TYPE_SCHEMA:
          MeasurementSchema.deserializeFrom(byteBuffer);
          break;
        case DATA_BLOCK_TYPE_TAGS:
        case DATA_BLOCK_TYPE_ATTRIBUTES:
          ReadWriteIOUtils.readMap(byteBuffer);
          break;
        default:
          break;
      }
      // got the data we need,don't need to read any more
      if (type == blockType) {
        typeExist = true;
        break;
      }
    }
    return typeExist ? index : -1;
  }

  public static byte[] updateTTL(byte[] origin, long ttl) {
    int index = indexOfDataBlockType(origin, DATA_BLOCK_TYPE_TTL);
    if (index < 1) {
      byte[] ttlBlock = new byte[Long.BYTES + 1];
      ttlBlock[0] = DATA_BLOCK_TYPE_TTL;
      BytesUtils.longToBytes(ttl, ttlBlock, 1);
      origin[1] = (byte) (origin[1] | FLAG_SET_TTL);
      return BytesUtils.concatByteArray(origin, ttlBlock);
    } else {
      BytesUtils.longToBytes(ttl, origin, index);
      return origin;
    }
  }

  private static final char START_FLAG = '\u0019';
  private static final char SPLIT_FLAG = '.';

  /**
   * parse value and return a specified type. if no data is required, null is returned.
   *
   * @param value value written in default table
   * @param type the type of value to obtain
   */
  public static Object parseNodeValue(byte[] value, byte type) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(value);
    // skip the version flag and node type flag
    ReadWriteIOUtils.readByte(byteBuffer);
    // get block type
    byte flag = ReadWriteIOUtils.readByte(byteBuffer);

    Object obj = null;
    // this means that the following data contains the information we need
    if ((flag & type) > 0) {
      while (byteBuffer.hasRemaining()) {
        byte blockType = ReadWriteIOUtils.readByte(byteBuffer);
        switch (blockType) {
          case DATA_BLOCK_TYPE_TTL:
            obj = ReadWriteIOUtils.readLong(byteBuffer);
            break;
          case DATA_BLOCK_TYPE_ALIAS:
            obj = ReadWriteIOUtils.readString(byteBuffer);
            break;
          case DATA_BLOCK_TYPE_ORIGIN_KEY:
            obj = readOriginKey(byteBuffer);
            break;
          case DATA_BLOCK_TYPE_SCHEMA:
            obj = MeasurementSchema.deserializeFrom(byteBuffer);
            break;
          case DATA_BLOCK_TYPE_TAGS:
          case DATA_BLOCK_TYPE_ATTRIBUTES:
            obj = ReadWriteIOUtils.readMap(byteBuffer);
            break;
          default:
            break;
        }
        // got the data we need,don't need to read any more
        if (type == blockType) {
          break;
        }
      }
    }
    return obj;
  }

  /**
   * get inner name by converting partial path.
   *
   * @param partialPath the path needed to be converted.
   * @param level the level needed to be added.
   * @param nodeType specified type
   * @return inner name
   */
  public static String convertPartialPathToInner(String partialPath, int level, char nodeType) {
    char lastChar = START_FLAG;
    StringBuilder stringBuilder = new StringBuilder();
    for (char c : partialPath.toCharArray()) {
      if (START_FLAG == lastChar) {
        stringBuilder.append(nodeType);
      }
      if (SPLIT_FLAG == lastChar) {
        stringBuilder.append(level);
      }
      stringBuilder.append(c);
      lastChar = c;
    }
    return stringBuilder.toString();
  }

  public static String convertPartialPathToInnerByNodes(String[] nodes, int level, char nodeType) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(nodeType).append(ROOT);
    for (String str : nodes) {
      stringBuilder.append(SPLIT_FLAG).append(level).append(str);
    }
    return stringBuilder.toString();
  }

  public static int getLevelByPartialPath(String partialPath) {
    int levelCount = 0;
    for (char c : partialPath.toCharArray()) {
      if (SPLIT_FLAG == c) {
        levelCount++;
      }
    }
    return levelCount;
  }

  public static byte[] getSuffixOfLevelPath(String[] nodes, int level) {
    StringBuilder stringBuilder = new StringBuilder();
    for (String str : nodes) {
      stringBuilder.append(PATH_SEPARATOR).append(level).append(str);
    }
    return stringBuilder.toString().getBytes();
  }

  public static boolean suffixMatch(byte[] key, byte[] suffix) {
    if (key.length < suffix.length) {
      return false;
    }

    for (int i = key.length - 1, j = suffix.length - 1; i >= 0 && j >= 0; ) {
      if ((key[i] ^ suffix[j]) != 0) {
        return false;
      }
      i--;
      j--;
    }
    return true;
  }

  public static boolean prefixMatch(byte[] key, byte[] prefix) {
    if (key.length < prefix.length) {
      return false;
    }

    for (int i = 0, j = 0; i < key.length && j < prefix.length; ) {
      if ((key[i] ^ prefix[j]) != 0) {
        return false;
      }
      i++;
      j++;
    }
    return true;
  }

  public static String[] toMetaNodes(byte[] rocksdbKey) {
    String rawKey = new String(BytesUtils.subBytes(rocksdbKey, 1, rocksdbKey.length - 1));
    String[] nodes = rawKey.split(ESCAPE_PATH_SEPARATOR);
    nodes[0] = ROOT_STRING;
    for (int i = 1; i < nodes.length; i++) {
      nodes[i] = nodes[i].substring(1);
    }
    return nodes;
  }

  /**
   * Statistics the number of all data entries for a specified column family
   *
   * @param rocksDB rocksdb
   * @param columnFamilyHandle specified column family handle
   * @return total number in this column family
   */
  public static long countNodesNum(RocksDB rocksDB, ColumnFamilyHandle columnFamilyHandle) {
    RocksIterator iter;
    if (columnFamilyHandle == null) {
      iter = rocksDB.newIterator();
    } else {
      iter = rocksDB.newIterator(columnFamilyHandle);
    }
    long count = 0;
    for (iter.seekToFirst(); iter.isValid(); iter.next()) {
      count++;
    }
    return count;
  }

  public static String getPathByInnerName(String innerName) {
    char[] keyConvertToCharArray = innerName.toCharArray();
    StringBuilder stringBuilder = new StringBuilder();
    char lastChar = START_FLAG;
    boolean replaceFlag = true;
    for (char c : keyConvertToCharArray) {
      if (SPLIT_FLAG == lastChar || START_FLAG == lastChar) {
        lastChar = c;
        continue;
      }
      if (ROOT_CHAR == c && replaceFlag) {
        lastChar = c;
        stringBuilder.append(ROOT_STRING);
        replaceFlag = false;
        continue;
      }
      stringBuilder.append(c);
      lastChar = c;
    }
    return stringBuilder.toString();
  }

  // eg. root.a.*.**.b.**.c
  public static List<String[]> replaceMultiWildcard(String[] nodes, int maxLevel)
      throws IllegalPathException {
    List<String[]> allResult = new ArrayList<>();
    List<Integer> multiWildcardPosition = new ArrayList<>();
    for (int i = 0; i < nodes.length; i++) {
      if (MULTI_LEVEL_PATH_WILDCARD.equals(nodes[i])) {
        multiWildcardPosition.add(i);
      }
    }
    if (multiWildcardPosition.isEmpty()) {
      allResult.add(nodes);
    } else if (multiWildcardPosition.size() == 1) {
      for (int i = 1; i <= maxLevel - nodes.length + 1; i++) {
        String[] clone = nodes.clone();
        clone[multiWildcardPosition.get(0)] = replaceWildcard(i);
        allResult.add(newStringArray(clone));
      }
    } else {
      List<int[]> result =
          getAllCompoundMode(maxLevel - multiWildcardPosition.size(), multiWildcardPosition.size());
      for (int[] value : result) {
        String[] clone = nodes.clone();
        for (int i = 0; i < value.length; i++) {
          clone[multiWildcardPosition.get(i)] = replaceWildcard(value[i]);
        }
        allResult.add(newStringArray(clone));
      }
    }
    return allResult;
  }

  private static String[] newStringArray(String[] oldArray) throws IllegalPathException {
    StringBuilder stringBuilder = new StringBuilder();
    for (String str : oldArray) {
      stringBuilder.append(PATH_SEPARATOR).append(str);
    }
    return MetaUtils.splitPathToDetachedPath(stringBuilder.substring(1));
  }

  private static String replaceWildcard(int num) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < num; i++) {
      stringBuilder.append(RockDBConstants.PATH_SEPARATOR).append(ONE_LEVEL_PATH_WILDCARD);
    }
    return stringBuilder.substring(1);
  }

  private static List<int[]> getAllCompoundMode(int sum, int n) {
    if (n <= 2) {
      List<int[]> result = new ArrayList<>();
      for (int i = 1; i < sum; i++) {
        result.add(new int[] {i, sum - i});
      }
      return result;
    }
    List<int[]> allResult = new ArrayList<>();
    for (int i = 1; i <= sum - n + 1; i++) {
      List<int[]> temp = getAllCompoundMode(sum - i, n - 1);
      for (int[] value : temp) {
        int[] result = new int[value.length + 1];
        result[0] = i;
        System.arraycopy(value, 0, result, 1, value.length);
        allResult.add(result);
      }
    }
    return allResult;
  }
}
