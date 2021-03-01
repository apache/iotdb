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
package org.apache.iotdb.db.index.common;

import org.apache.iotdb.db.metadata.MetadataOperationType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class IndexInfo implements Cloneable {

  private Map<String, String> props;
  private long time;
  private IndexType indexType;

  public IndexInfo(IndexType indexType, long time, Map<String, String> props) {
    this.props = props;
    this.time = time;
    this.indexType = indexType;
  }

  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  @Deprecated
  public String serializeCreateIndex(String path) {
    StringBuilder res = new StringBuilder();
    res.append(
        String.format(
            "%s,%s,%s,%s", MetadataOperationType.CREATE_INDEX, path, indexType.serialize(), time));
    if (props != null && !props.isEmpty()) {
      for (Map.Entry entry : props.entrySet()) {
        res.append(String.format(",%s=%s", entry.getKey(), entry.getValue()));
      }
    }
    return res.toString();
  }

  /**
   * @param args [0] is the MetadataType, [1] is the path, the rest is to be parsed.
   * @return parsed IndexInfo
   */
  @Deprecated
  public static IndexInfo deserializeCreateIndex(String[] args) {
    IndexType indexType = IndexType.deserialize(Short.parseShort(args[2]));
    long time = Long.parseLong(args[3]);
    HashMap<String, String> indexProps = null;
    if (args.length > 4) {
      String[] kv;
      indexProps = new HashMap<>(args.length - 4 + 1, 1);
      for (int k = 4; k < args.length; k++) {
        kv = args[k].split("=");
        indexProps.put(kv[0], kv[1]);
      }
    }
    return new IndexInfo(indexType, time, indexProps);
  }

  @Deprecated
  public static String serializeDropIndex(String path, IndexType indexType) {
    return String.format("%s,%s,%s", MetadataOperationType.DROP_INDEX, path, indexType.serialize());
  }

  /**
   * @param args [0] is the MetadataType, [1] is the path, the rest is to be parsed.
   * @return parsed IndexInfo
   */
  @Deprecated
  public static IndexType deserializeDropIndex(String[] args) {
    return IndexType.deserialize(Short.parseShort(args[2]));
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(indexType.serialize(), outputStream);
    ReadWriteIOUtils.write(time, outputStream);
    ReadWriteIOUtils.write(props, outputStream);
  }

  public static IndexInfo deserialize(InputStream inputStream) throws IOException {
    short indexTypeShort = ReadWriteIOUtils.readShort(inputStream);
    IndexType indexType = IndexType.deserialize(indexTypeShort);
    long time = ReadWriteIOUtils.readLong(inputStream);
    Map<String, String> indexProps = ReadWriteIOUtils.readMap(inputStream);
    return new IndexInfo(indexType, time, indexProps);
  }

  @Override
  public String toString() {
    return String.format("[type: %s, time: %d, props: %s]", indexType, time, props);
  }

  @Override
  public Object clone() {
    return new IndexInfo(indexType, time, new HashMap<>(props));
  }
}
