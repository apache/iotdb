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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry;

import org.apache.iotdb.lsm.sstable.bplustree.entry.IEntry;

import org.roaringbitmap.InvalidRoaringFormat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class RoaringBitmapHeader implements IEntry {

  int cookie;

  int size;

  char[] keys;

  int[] cardinalities;

  public int getCookie() {
    return cookie;
  }

  public void setCookie(int cookie) {
    this.cookie = cookie;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public char[] getKeys() {
    return keys;
  }

  public void setKeys(char[] keys) {
    this.keys = keys;
  }

  public int[] getCardinalities() {
    return cardinalities;
  }

  public void setCardinalities(int[] cardinalities) {
    this.cardinalities = cardinalities;
  }

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    // do nothing
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    // do nothing
  }

  @Override
  public IEntry deserialize(DataInputStream in) throws IOException {
    this.cookie = Integer.reverseBytes(in.readInt());
    if ((cookie & '\uffff') != 12347 && cookie != 12346) {
      throw new InvalidRoaringFormat("find a valid cookie.");
    } else {
      this.size =
          (cookie & '\uffff') == 12347 ? (cookie >>> 16) + 1 : Integer.reverseBytes(in.readInt());
      if (this.size > 65536) {
        throw new InvalidRoaringFormat("Size too large");
      } else {
        this.keys = new char[this.size];
        this.cardinalities = new int[this.size];

        int k;
        for (k = 0; k < this.size; ++k) {
          keys[k] = Character.reverseBytes(in.readChar());
          cardinalities[k] = 1 + ('\uffff' & Character.reverseBytes(in.readChar()));
        }
      }
    }
    return this;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException("deserialize RoaringBitmapHeader from byte buffer");
  }

  @Override
  public String toString() {
    return "RoaringBitmapHeader{"
        + "cookie="
        + cookie
        + ", size="
        + size
        + ", keys="
        + Arrays.toString(keys)
        + ", cardinalities="
        + Arrays.toString(cardinalities)
        + '}';
  }

  public boolean hasRun() {
    return (cookie & '\uffff') == 12347;
  }
}
