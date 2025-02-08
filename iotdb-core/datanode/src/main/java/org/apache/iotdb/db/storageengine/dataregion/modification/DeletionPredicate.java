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
package org.apache.iotdb.db.storageengine.dataregion.modification;

import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.NOP;
import org.apache.iotdb.db.utils.io.BufferSerializable;
import org.apache.iotdb.db.utils.io.StreamSerializable;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DeletionPredicate implements StreamSerializable, BufferSerializable {

  private String tableName;
  private IDPredicate idPredicate = new NOP();
  // an empty list means affecting all columns
  private List<String> measurementNames = Collections.emptyList();

  public DeletionPredicate() {}

  public DeletionPredicate(String tableName) {
    this.tableName = tableName;
  }

  public DeletionPredicate(String tableName, IDPredicate idPredicate) {
    this.tableName = tableName;
    this.idPredicate = idPredicate;
  }

  public DeletionPredicate(
      String tableName, IDPredicate idPredicate, List<String> measurementNames) {
    this.tableName = tableName;
    this.idPredicate = idPredicate;
    this.measurementNames = measurementNames;
  }

  public boolean matches(IDeviceID deviceID) {
    return tableName.equals(deviceID.getTableName()) && idPredicate.matches(deviceID);
  }

  public void setIdPredicate(IDPredicate idPredicate) {
    this.idPredicate = idPredicate;
  }

  public String getTableName() {
    return tableName;
  }

  public List<String> getMeasurementNames() {
    return measurementNames;
  }

  public boolean affects(String measurementName) {
    return measurementNames.isEmpty() || measurementNames.contains(measurementName);
  }

  @Override
  public long serialize(OutputStream stream) throws IOException {
    long size = ReadWriteIOUtils.writeVar(tableName, stream);
    size += idPredicate.serialize(stream);
    size += ReadWriteForEncodingUtils.writeVarInt(measurementNames.size(), stream);
    for (String measurementName : measurementNames) {
      size += ReadWriteIOUtils.writeVar(measurementName, stream);
    }
    return size;
  }

  @Override
  public long serialize(ByteBuffer buffer) {
    long size = ReadWriteIOUtils.writeVar(tableName, buffer);
    size += idPredicate.serialize(buffer);
    size += ReadWriteForEncodingUtils.writeVarInt(measurementNames.size(), buffer);
    for (String measurementName : measurementNames) {
      size += ReadWriteIOUtils.writeVar(measurementName, buffer);
    }
    return size;
  }

  @Override
  public void deserialize(InputStream stream) throws IOException {
    tableName = ReadWriteIOUtils.readVarIntString(stream);
    idPredicate = IDPredicate.createFrom(stream);

    int measurementLength = ReadWriteForEncodingUtils.readVarInt(stream);
    if (measurementLength > 0) {
      measurementNames = new ArrayList<>(measurementLength);
      for (int i = 0; i < measurementLength; i++) {
        measurementNames.add(ReadWriteIOUtils.readVarIntString(stream));
      }
    } else {
      measurementNames = Collections.emptyList();
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    tableName = ReadWriteIOUtils.readVarIntString(buffer);
    idPredicate = IDPredicate.createFrom(buffer);

    int measurementLength = ReadWriteForEncodingUtils.readVarInt(buffer);
    if (measurementLength > 0) {
      measurementNames = new ArrayList<>(measurementLength);
      for (int i = 0; i < measurementLength; i++) {
        measurementNames.add(ReadWriteIOUtils.readVarIntString(buffer));
      }
    } else {
      measurementNames = Collections.emptyList();
    }
  }

  public int serializedSize() {
    // table name + id predicate +
    int size =
        ReadWriteForEncodingUtils.varIntSize(tableName.length())
            + tableName.length() * Character.BYTES
            + idPredicate.serializedSize()
            + ReadWriteForEncodingUtils.varIntSize(measurementNames.size());
    for (String measurementName : measurementNames) {
      size +=
          ReadWriteForEncodingUtils.varIntSize(
              measurementName.length() * measurementName.length() * Character.BYTES);
    }
    return size;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeletionPredicate that = (DeletionPredicate) o;
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(idPredicate, that.idPredicate)
        && Objects.equals(measurementNames, that.measurementNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, idPredicate, measurementNames);
  }

  @Override
  public String toString() {
    return "DeletionPredicate{"
        + "tableName='"
        + tableName
        + '\''
        + ", idPredicate="
        + idPredicate
        + ", measurementNames="
        + measurementNames
        + '}';
  }
}
