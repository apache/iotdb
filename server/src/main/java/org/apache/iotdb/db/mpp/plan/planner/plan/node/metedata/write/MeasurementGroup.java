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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MeasurementGroup {

  private List<String> measurements = new ArrayList<>();
  private List<TSDataType> dataTypes = new ArrayList<>();
  private List<TSEncoding> encodings = new ArrayList<>();
  private List<CompressionType> compressors = new ArrayList<>();
  private List<String> aliasList;
  private List<Map<String, String>> propsList;
  private List<Map<String, String>> tagsList;
  private List<Map<String, String>> attributesList;

  public List<String> getMeasurements() {
    return measurements;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public List<TSEncoding> getEncodings() {
    return encodings;
  }

  public List<CompressionType> getCompressors() {
    return compressors;
  }

  public List<String> getAliasList() {
    return aliasList;
  }

  public List<Map<String, String>> getPropsList() {
    return propsList;
  }

  public List<Map<String, String>> getTagsList() {
    return tagsList;
  }

  public void setTagsList(List<Map<String, String>> tagsList) {
    this.tagsList = tagsList;
  }

  public List<Map<String, String>> getAttributesList() {
    return attributesList;
  }

  public void addMeasurement(
      String measurement,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressionType) {
    measurements.add(measurement);
    dataTypes.add(dataType);
    encodings.add(encoding);
    compressors.add(compressionType);
  }

  public void addAlias(String alias) {
    if (aliasList == null) {
      aliasList = new ArrayList<>();
    }
    aliasList.add(alias);
  }

  public void addProps(Map<String, String> props) {
    if (propsList == null) {
      propsList = new ArrayList<>();
    }
    propsList.add(props);
  }

  public void addTags(Map<String, String> tags) {
    if (tagsList == null) {
      tagsList = new ArrayList<>();
    }
    tagsList.add(tags);
  }

  public void addAttributes(Map<String, String> attributes) {
    if (attributesList == null) {
      attributesList = new ArrayList<>();
    }
    attributesList.add(attributes);
  }

  public void serialize(ByteBuffer byteBuffer) {
    // measurements
    ReadWriteIOUtils.write(measurements.size(), byteBuffer);
    for (String measurement : measurements) {
      ReadWriteIOUtils.write(measurement, byteBuffer);
    }

    // dataTypes
    for (TSDataType dataType : dataTypes) {
      byteBuffer.put((byte) dataType.ordinal());
    }

    // encodings
    for (TSEncoding encoding : encodings) {
      byteBuffer.put((byte) encoding.ordinal());
    }

    // compressors
    for (CompressionType compressor : compressors) {
      byteBuffer.put((byte) compressor.ordinal());
    }

    // alias
    if (aliasList == null) {
      byteBuffer.put((byte) -1);
    } else if (aliasList.isEmpty()) {
      byteBuffer.put((byte) 0);
    } else {
      byteBuffer.put((byte) 1);
      for (String alias : aliasList) {
        ReadWriteIOUtils.write(alias, byteBuffer);
      }
    }

    // tags
    if (tagsList == null) {
      byteBuffer.put((byte) -1);
    } else if (tagsList.isEmpty()) {
      byteBuffer.put((byte) 0);
    } else {
      byteBuffer.put((byte) 1);
      for (Map<String, String> tags : tagsList) {
        ReadWriteIOUtils.write(tags, byteBuffer);
      }
    }

    // attributes
    if (attributesList == null) {
      byteBuffer.put((byte) -1);
    } else if (attributesList.isEmpty()) {
      byteBuffer.put((byte) 0);
    } else {
      byteBuffer.put((byte) 1);
      for (Map<String, String> attributes : attributesList) {
        ReadWriteIOUtils.write(attributes, byteBuffer);
      }
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    // measurements
    ReadWriteIOUtils.write(measurements.size(), stream);
    for (String measurement : measurements) {
      ReadWriteIOUtils.write(measurement, stream);
    }

    // dataTypes
    for (TSDataType dataType : dataTypes) {
      stream.write((byte) dataType.ordinal());
    }

    // encodings
    for (TSEncoding encoding : encodings) {
      stream.write((byte) encoding.ordinal());
    }

    // compressors
    for (CompressionType compressor : compressors) {
      stream.write((byte) compressor.ordinal());
    }

    // alias
    if (aliasList == null) {
      stream.write((byte) -1);
    } else if (aliasList.isEmpty()) {
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      for (String alias : aliasList) {
        ReadWriteIOUtils.write(alias, stream);
      }
    }

    // tags
    if (tagsList == null) {
      stream.write((byte) -1);
    } else if (tagsList.isEmpty()) {
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      for (Map<String, String> tags : tagsList) {
        ReadWriteIOUtils.write(tags, stream);
      }
    }

    // attributes
    if (attributesList == null) {
      stream.write((byte) -1);
    } else if (attributesList.isEmpty()) {
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      for (Map<String, String> attributes : attributesList) {
        ReadWriteIOUtils.write(attributes, stream);
      }
    }
  }

  public void deserialize(ByteBuffer byteBuffer) {
    measurements = new ArrayList<>();
    int size = byteBuffer.getInt();
    for (int i = 0; i < size; i++) {
      measurements.add(ReadWriteIOUtils.readString(byteBuffer));
    }

    dataTypes = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      dataTypes.add(TSDataType.values()[byteBuffer.get()]);
    }

    encodings = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      encodings.add(TSEncoding.values()[byteBuffer.get()]);
    }

    compressors = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      compressors.add(CompressionType.values()[byteBuffer.get()]);
    }

    byte label = byteBuffer.get();
    if (label >= 0) {
      aliasList = new ArrayList<>();
      if (label == 1) {
        for (int i = 0; i < size; i++) {
          aliasList.add(ReadWriteIOUtils.readString(byteBuffer));
        }
      }
    }

    label = byteBuffer.get();
    if (label >= 0) {
      tagsList = new ArrayList<>();
      if (label == 1) {
        for (int i = 0; i < size; i++) {
          tagsList.add(ReadWriteIOUtils.readMap(byteBuffer));
        }
      }
    }

    label = byteBuffer.get();
    if (label >= 0) {
      attributesList = new ArrayList<>();
      if (label == 1) {
        for (int i = 0; i < size; i++) {
          attributesList.add(ReadWriteIOUtils.readMap(byteBuffer));
        }
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MeasurementGroup that = (MeasurementGroup) o;
    return Objects.equals(measurements, that.measurements)
        && Objects.equals(dataTypes, that.dataTypes)
        && Objects.equals(encodings, that.encodings)
        && Objects.equals(compressors, that.compressors)
        && Objects.equals(aliasList, that.aliasList)
        && Objects.equals(propsList, that.propsList)
        && Objects.equals(tagsList, that.tagsList)
        && Objects.equals(attributesList, that.attributesList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        measurements,
        dataTypes,
        encodings,
        compressors,
        aliasList,
        propsList,
        tagsList,
        attributesList);
  }
}
