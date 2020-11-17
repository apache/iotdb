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
package org.apache.iotdb.db.qp.physical.sys;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * create multiple timeSeries, could be split to several sub Plans to execute in different DataGroup
 */
public class CreateMultiTimeSeriesPlan extends PhysicalPlan {
  private List<PartialPath> paths;
  private List<TSDataType> dataTypes;
  private List<TSEncoding> encodings;
  private List<CompressionType> compressors;
  private List<String> alias = null;
  private List<Map<String, String>> props = null;
  private List<Map<String, String>> tags = null;
  private List<Map<String, String>> attributes = null;

  /*
   ** record the result of creation of time series
   */
  private Map<Integer, Exception> results = new HashMap<>();
  private List<Integer> indexes;

  public CreateMultiTimeSeriesPlan() {
    super(false, Operator.OperatorType.CREATE_MULTI_TIMESERIES);
  }

  @Override
  public List<PartialPath> getPaths() {
    return paths;
  }

  public void setPaths(List<PartialPath> paths) {
    this.paths = paths;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public List<TSEncoding> getEncodings() {
    return encodings;
  }

  public void setEncodings(List<TSEncoding> encodings) {
    this.encodings = encodings;
  }

  public List<CompressionType> getCompressors() {
    return compressors;
  }

  public void setCompressors(List<CompressionType> compressors) {
    this.compressors = compressors;
  }

  public List<String> getAlias() {
    return alias;
  }

  public void setAlias(List<String> alias) {
    this.alias = alias;
  }

  public List<Map<String, String>> getProps() {
    return props;
  }

  public void setProps(List<Map<String, String>> props) {
    this.props = props;
  }

  public List<Map<String, String>> getTags() {
    return tags;
  }

  public void setTags(List<Map<String, String>> tags) {
    this.tags = tags;
  }

  public List<Map<String, String>> getAttributes() {
    return attributes;
  }

  public void setAttributes(List<Map<String, String>> attributes) {
    this.attributes = attributes;
  }

  public List<Integer> getIndexes() {
    return indexes;
  }

  public void setIndexes(List<Integer> indexes) {
    this.indexes = indexes;
  }

  public Map<Integer, Exception> getResults() {
    return results;
  }

  public void setResults(Map<Integer, Exception> results) {
    this.results = results;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.MULTI_CREATE_TIMESERIES.ordinal();
    stream.write(type);
    stream.writeInt(paths.size());

    for (PartialPath path : paths) {
      putString(stream, path.getFullPath());
    }

    for (TSDataType dataType : dataTypes) {
      stream.write(dataType.ordinal());
    }

    for (TSEncoding encoding : encodings) {
      stream.write(encoding.ordinal());
    }

    for (CompressionType compressor : compressors) {
      stream.write(compressor.ordinal());
    }

    if (alias != null) {
      stream.write(1);
      for (String name : alias) {
        putString(stream, name);
      }
    } else {
      stream.write(0);
    }

    if (props != null) {
      stream.write(1);
      for (Map<String, String> prop : props) {
        ReadWriteIOUtils.write(prop, stream);
      }
    } else {
      stream.write(0);
    }

    if (tags != null) {
      stream.write(1);
      for (Map<String, String> tag : tags) {
        ReadWriteIOUtils.write(tag, stream);
      }
    } else {
      stream.write(0);
    }

    if (attributes != null) {
      stream.write(1);
      for (Map<String, String> attribute : attributes) {
        ReadWriteIOUtils.write(attribute, stream);
      }
    } else {
      stream.write(0);
    }

    stream.writeLong(index);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.MULTI_CREATE_TIMESERIES.ordinal();
    buffer.put((byte) type);
    buffer.putInt(paths.size());

    for (PartialPath path : paths) {
      putString(buffer, path.getFullPath());
    }

    for (TSDataType dataType : dataTypes) {
      buffer.put((byte) dataType.ordinal());
    }

    for (TSEncoding encoding : encodings) {
      buffer.put((byte) encoding.ordinal());
    }

    for (CompressionType compressor : compressors) {
      buffer.put((byte) compressor.ordinal());
    }

    if (alias != null) {
      buffer.put((byte) 1);
      for (String name : alias) {
        putString(buffer, name);
      }
    } else {
      buffer.put((byte) 0);
    }

    if (props != null) {
      buffer.put((byte) 1);
      for (Map<String, String> prop : props) {
        ReadWriteIOUtils.write(prop, buffer);
      }
    } else {
      buffer.put((byte) 0);
    }

    if (tags != null) {
      buffer.put((byte) 1);
      for (Map<String, String> tag : tags) {
        ReadWriteIOUtils.write(tag, buffer);
      }
    } else {
      buffer.put((byte) 0);
    }

    if (attributes != null) {
      buffer.put((byte) 1);
      for (Map<String, String> attribute : attributes) {
        ReadWriteIOUtils.write(attribute, buffer);
      }
    } else {
      buffer.put((byte) 0);
    }

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    int totalSize = buffer.getInt();
    paths = new ArrayList<>(totalSize);
    for (int i = 0; i < totalSize; i++) {
      paths.add(new PartialPath(readString(buffer)));
    }
    dataTypes = new ArrayList<>(totalSize);
    for (int i = 0; i < totalSize; i++) {
      dataTypes.add(TSDataType.values()[buffer.get()]);
    }
    encodings = new ArrayList<>(totalSize);
    for (int i = 0; i < totalSize; i++) {
      encodings.add(TSEncoding.values()[buffer.get()]);
    }

    if (buffer.get() == 1) {
      alias = new ArrayList<>(totalSize);
      for (int i = 0; i < totalSize; i++) {
        alias.add(readString(buffer));
      }
    }

    if (buffer.get() == 1) {
      props = new ArrayList<>(totalSize);
      for (int i = 0; i < totalSize; i++) {
        props.add(ReadWriteIOUtils.readMap(buffer));
      }
    }

    if (buffer.get() == 1) {
      tags = new ArrayList<>(totalSize);
      for (int i = 0; i < totalSize; i++) {
        tags.add(ReadWriteIOUtils.readMap(buffer));
      }
    }

    if (buffer.get() == 1) {
      attributes = new ArrayList<>(totalSize);
      for (int i = 0; i < totalSize; i++) {
        attributes.add(ReadWriteIOUtils.readMap(buffer));
      }
    }

    this.index = buffer.getLong();
  }
}
