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

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Deserializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class IDPredicate implements StreamSerializable, BufferSerializable {

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
