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

import org.apache.iotdb.db.utils.io.BufferSerializable;
import org.apache.iotdb.db.utils.io.StreamSerializable;

import org.apache.tsfile.common.conf.TSFileConfig;
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

public abstract class IDPredicate implements StreamSerializable, BufferSerializable {

  public int serializedSize() {
    // type
    return Byte.BYTES;
  }

  @SuppressWarnings("java:S6548")
  public enum IDPredicateType {
    NOP,
    FULL_EXACT_MATCH,
    SEGMENT_EXACT_MATCH,
    AND;

    public long serialize(OutputStream stream) throws IOException {
      stream.write((byte) ordinal());
      return 1;
    }

    public long serialize(ByteBuffer buffer) {
      buffer.put((byte) ordinal());
      return 1;
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
  public long serialize(OutputStream stream) throws IOException {
    return type.serialize(stream);
  }

  @Override
  public long serialize(ByteBuffer buffer) {
    return type.serialize(buffer);
  }

  public static IDPredicate createFrom(ByteBuffer buffer) {
    IDPredicateType type = IDPredicateType.deserialize(buffer);
    IDPredicate predicate;
    if (Objects.requireNonNull(type) == IDPredicateType.NOP) {
      predicate = new NOP();
    } else if (Objects.requireNonNull(type) == IDPredicateType.FULL_EXACT_MATCH) {
      predicate = new FullExactMatch();
    } else if (Objects.requireNonNull(type) == IDPredicateType.SEGMENT_EXACT_MATCH) {
      predicate = new SegmentExactMatch();
    } else if (Objects.requireNonNull(type) == IDPredicateType.AND) {
      predicate = new And();
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
    } else if (Objects.requireNonNull(type) == IDPredicateType.FULL_EXACT_MATCH) {
      predicate = new FullExactMatch();
    } else if (Objects.requireNonNull(type) == IDPredicateType.SEGMENT_EXACT_MATCH) {
      predicate = new SegmentExactMatch();
    } else if (Objects.requireNonNull(type) == IDPredicateType.AND) {
      predicate = new And();
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

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof NOP;
    }

    @Override
    public String toString() {
      return "NOP";
    }
  }

  public static class FullExactMatch extends IDPredicate {

    private IDeviceID deviceID;

    public FullExactMatch(IDeviceID deviceID) {
      super(IDPredicateType.FULL_EXACT_MATCH);
      this.deviceID = deviceID;
    }

    public FullExactMatch() {
      super(IDPredicateType.FULL_EXACT_MATCH);
    }

    @Override
    public int serializedSize() {
      return super.serializedSize() + deviceID.serializedSize();
    }

    @Override
    public long serialize(OutputStream stream) throws IOException {
      long size = super.serialize(stream);
      size += deviceID.serialize(stream);
      return size;
    }

    @Override
    public long serialize(ByteBuffer buffer) {
      long size = super.serialize(buffer);
      size += deviceID.serialize(buffer);
      return size;
    }

    @Override
    public void deserialize(InputStream stream) throws IOException {
      deviceID = Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(stream);
    }

    @Override
    public void deserialize(ByteBuffer buffer) {
      deviceID = Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(buffer);
    }

    @Override
    public boolean matches(IDeviceID deviceID) {
      return this.deviceID.equals(deviceID);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FullExactMatch that = (FullExactMatch) o;
      return Objects.equals(deviceID, that.deviceID);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(deviceID);
    }

    @Override
    public String toString() {
      return "FullExactMatch{" + "deviceID=" + deviceID + '}';
    }
  }

  public static class SegmentExactMatch extends IDPredicate {

    private String pattern;
    private int segmentIndex;

    public SegmentExactMatch(String pattern, int segmentIndex) {
      super(IDPredicateType.SEGMENT_EXACT_MATCH);
      this.pattern = pattern;
      this.segmentIndex = segmentIndex;
    }

    public SegmentExactMatch() {
      super(IDPredicateType.SEGMENT_EXACT_MATCH);
    }

    @Override
    public int serializedSize() {
      if (pattern != null) {
        byte[] bytes = pattern.getBytes(TSFileConfig.STRING_CHARSET);
        return super.serializedSize()
            + ReadWriteForEncodingUtils.varIntSize(bytes.length)
            + bytes.length * Character.BYTES
            + ReadWriteForEncodingUtils.varIntSize(segmentIndex);
      } else {
        return ReadWriteForEncodingUtils.varIntSize(-1);
      }
    }

    @Override
    public long serialize(OutputStream stream) throws IOException {
      long size = super.serialize(stream);
      size += ReadWriteIOUtils.writeVar(pattern, stream);
      size += ReadWriteForEncodingUtils.writeVarInt(segmentIndex, stream);
      return size;
    }

    @Override
    public long serialize(ByteBuffer buffer) {
      long size = super.serialize(buffer);
      size += ReadWriteIOUtils.writeVar(pattern, buffer);
      size += ReadWriteForEncodingUtils.writeVarInt(segmentIndex, buffer);
      return size;
    }

    @Override
    public void deserialize(InputStream stream) throws IOException {
      pattern = ReadWriteIOUtils.readVarIntString(stream);
      segmentIndex = ReadWriteForEncodingUtils.readVarInt(stream);
    }

    @Override
    public void deserialize(ByteBuffer buffer) {
      pattern = ReadWriteIOUtils.readVarIntString(buffer);
      segmentIndex = ReadWriteForEncodingUtils.readVarInt(buffer);
    }

    @Override
    public boolean matches(IDeviceID deviceID) {
      return Objects.equals(pattern, deviceID.segment(segmentIndex));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SegmentExactMatch that = (SegmentExactMatch) o;
      return segmentIndex == that.segmentIndex && Objects.equals(pattern, that.pattern);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pattern, segmentIndex);
    }

    @Override
    public String toString() {
      return "SegmentExactMatch{"
          + "pattern='"
          + pattern
          + '\''
          + ", segmentIndex="
          + segmentIndex
          + '}';
    }
  }

  public static class And extends IDPredicate {

    private final List<IDPredicate> predicates = new ArrayList<>();

    public And(IDPredicate... predicates) {
      super(IDPredicateType.AND);
      Collections.addAll(this.predicates, predicates);
    }

    public void add(IDPredicate predicate) {
      predicates.add(predicate);
    }

    @Override
    public int serializedSize() {
      int serializedSize = super.serializedSize();
      serializedSize += ReadWriteForEncodingUtils.varIntSize(predicates.size());
      for (IDPredicate predicate : predicates) {
        serializedSize += predicate.serializedSize();
      }
      return serializedSize;
    }

    @Override
    public long serialize(OutputStream stream) throws IOException {
      long size = super.serialize(stream);
      size += ReadWriteForEncodingUtils.writeVarInt(predicates.size(), stream);
      for (IDPredicate predicate : predicates) {
        size += predicate.serialize(stream);
      }
      return size;
    }

    @Override
    public long serialize(ByteBuffer buffer) {
      long size = super.serialize(buffer);
      size += ReadWriteForEncodingUtils.writeVarInt(predicates.size(), buffer);
      for (IDPredicate predicate : predicates) {
        size += predicate.serialize(buffer);
      }
      return size;
    }

    @Override
    public void deserialize(InputStream stream) throws IOException {
      int size = ReadWriteForEncodingUtils.readVarInt(stream);
      for (int i = 0; i < size; i++) {
        predicates.add(IDPredicate.createFrom(stream));
      }
    }

    @Override
    public void deserialize(ByteBuffer buffer) {
      int size = ReadWriteForEncodingUtils.readVarInt(buffer);
      for (int i = 0; i < size; i++) {
        predicates.add(IDPredicate.createFrom(buffer));
      }
    }

    @Override
    public boolean matches(IDeviceID deviceID) {
      return predicates.stream().allMatch(predicate -> predicate.matches(deviceID));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      And and = (And) o;
      return Objects.equals(predicates, and.predicates);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(predicates);
    }

    @Override
    public String toString() {
      return "And{" + "predicates=" + predicates + '}';
    }
  }
}
