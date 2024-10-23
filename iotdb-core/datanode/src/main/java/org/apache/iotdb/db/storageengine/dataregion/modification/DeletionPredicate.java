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

import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate.IDPredicate.NOP;
import org.apache.iotdb.db.utils.io.BufferSerializable;
import org.apache.iotdb.db.utils.io.StreamSerializable;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Deserializer;
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
  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.writeVar(tableName, stream);
    idPredicate.serialize(stream);
    ReadWriteForEncodingUtils.writeVarInt(measurementNames.size(), stream);
    for (String measurementName : measurementNames) {
      ReadWriteIOUtils.writeVar(measurementName, stream);
    }
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.writeVar(tableName, buffer);
    idPredicate.serialize(buffer);
    ReadWriteForEncodingUtils.writeVarInt(measurementNames.size(), buffer);
    for (String measurementName : measurementNames) {
      ReadWriteIOUtils.writeVar(measurementName, buffer);
    }
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

  public abstract static class IDPredicate implements StreamSerializable, BufferSerializable {

    public int serializedSize() {
      // type
      return Byte.BYTES;
    }

    @SuppressWarnings("java:S6548")
    public enum IDPredicateType {
      NOP,
      FULL_EXACT_MATCH;

      public void serialize(OutputStream stream) throws IOException {
        stream.write((byte) ordinal());
      }

      public void serialize(ByteBuffer buffer) {
        buffer.put((byte) ordinal());
      }

      public static IDPredicateType deserialize(InputStream stream) throws IOException {
        return values()[stream.read()];
      }

      public static IDPredicateType deserialize(ByteBuffer buffer) {
        return values()[buffer.get()];
      }
    }

    protected final IDPredicateType type;

    protected IDPredicate(IDPredicateType type) {
      this.type = type;
    }

    public abstract boolean matches(IDeviceID deviceID);

    @Override
    public void serialize(OutputStream stream) throws IOException {
      type.serialize(stream);
    }

    @Override
    public void serialize(ByteBuffer buffer) {
      type.serialize(buffer);
    }

    public static IDPredicate createFrom(ByteBuffer buffer) {
      IDPredicateType type = IDPredicateType.deserialize(buffer);
      IDPredicate predicate;
      if (Objects.requireNonNull(type) == IDPredicateType.NOP) {
        predicate = new NOP();
      } else {
        throw new IllegalArgumentException("Unrecognized predicate type: " + type);
      }
      predicate.deserialize(buffer);
      return predicate;
    }

    public static IDPredicate createFrom(InputStream stream) throws IOException {
      IDPredicateType type = IDPredicateType.deserialize(stream);
      IDPredicate predicate;
      if (Objects.requireNonNull(type) == IDPredicateType.NOP) {
        predicate = new NOP();
      } else {
        throw new IllegalArgumentException("Unrecognized predicate type: " + type);
      }
      predicate.deserialize(stream);
      return predicate;
    }

    public static class NOP extends IDPredicate {

      public NOP() {
        super(IDPredicateType.NOP);
      }

      @Override
      public void serialize(OutputStream stream) {
        // nothing to be done
      }

      @Override
      public void serialize(ByteBuffer buffer) {
        // nothing to be done
      }

      @Override
      public void deserialize(InputStream stream) {
        // nothing to be done
      }

      @Override
      public void deserialize(ByteBuffer buffer) {
        // nothing to be done
      }

      @Override
      public boolean matches(IDeviceID deviceID) {
        return true;
      }
    }

    public static class FullExactMatch extends IDPredicate {
      private IDeviceID deviceID;

      public FullExactMatch(IDeviceID deviceID) {
        super(IDPredicateType.FULL_EXACT_MATCH);
        this.deviceID = deviceID;
      }

      @Override
      public int serializedSize() {
        return super.serializedSize() + deviceID.serializedSize();
      }

      @Override
      public void serialize(OutputStream stream) throws IOException {
        deviceID.serialize(stream);
      }

      @Override
      public void serialize(ByteBuffer buffer) {
        deviceID.serialize(buffer);
      }

      @Override
      public void deserialize(InputStream stream) throws IOException {
        Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(stream);
      }

      @Override
      public void deserialize(ByteBuffer buffer) {
        Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(buffer);
      }

      @Override
      public boolean matches(IDeviceID deviceID) {
        return this.deviceID.equals(deviceID);
      }
    }
  }
}
