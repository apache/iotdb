/**
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
package org.apache.iotdb.db.utils.datastructure;

import static org.apache.iotdb.db.utils.datastructure.ByteArrayPool.ARRAY_SIZE;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.tsfile.utils.PublicBAOS;


/**
 * We reimplement {@linkplain PublicBAOS}, replacing the underlying {@linkplain
 * java.io.ByteArrayOutputStream} with a {@code List } of {@code byte[]}.
 * <p>
 * For efficient and controllable GC, all {@code byte[]} are allocated from {@linkplain
 * ByteArrayPool} and should be put back after {@code close}.
 * <p>
 * Referring to {@linkplain TVList} and {@linkplain org.apache.iotdb.db.rescon.PrimitiveArrayPool PrimitiveArrayPool}.
 * <p>
 * So far, this class is only used in {@linkplain org.apache.iotdb.db.engine.memtable.MemTableFlushTask
 * MemTableFlushTask}.
 *
 * @author Pengze Lv, kangrong
 */
public class ListPublicBAOS extends PublicBAOS {

  /**
   * data buffer
   */
  private List<byte[]> values;

  /**
   * The number of valid bytes in the buffer.
   */
  private int dataSize;

  /**
   * capacity of the buffer.
   */
  private int capacity;

  public ListPublicBAOS() {
    super();

    values = new ArrayList<>();
    dataSize = 0;
    capacity = 0;
  }

  public ListPublicBAOS(int size) {
    super();
    if (size < 0) {
      throw new IllegalArgumentException("Negative initial size: "
              + size);
    }

    values = ByteArrayPool.getInstance().getByteLists(size);
    dataSize = 0;
    capacity = size;
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) {
    if (len == 0) return;
    if ((off < 0) || (off > b.length) || (len < 0) ||
            ((off + len) - b.length > 0)) {
      throw new IndexOutOfBoundsException();
    }
    ensureCapacity(dataSize + len);

    int arrayIndex = dataSize / ARRAY_SIZE;
    int elementIndex = dataSize % ARRAY_SIZE;
    byte[] insertedArray = values.get(arrayIndex);
    for (int i = off; i < off + len; i++) {
      if (elementIndex == ARRAY_SIZE) {
        elementIndex = 0;
        arrayIndex++;
        insertedArray = values.get(arrayIndex);
      }
      insertedArray[elementIndex] = b[i];
      elementIndex++;
    }
    dataSize += len;
  }

  @Override
  public synchronized void write(int b) {
    ensureCapacity(dataSize + 1);
    int arrayIndex = dataSize / ARRAY_SIZE;
    int elementIndex = dataSize % ARRAY_SIZE;
    values.get(arrayIndex)[elementIndex] = (byte) b;
    dataSize++;
  }

  /**
   * Increases the capacity if necessary to ensure that it can hold
   * at least the number of elements specified by the minimum
   * capacity argument.
   */
  private void ensureCapacity(int minCapacity) {
    while (minCapacity - capacity > 0) {
      values.add(ByteArrayPool.getInstance().getPrimitiveByteList());
      capacity += ARRAY_SIZE;
    }
  }

  public synchronized void reset() {
    dataSize = 0;
    if (values != null) {
      for (byte[] dataArray : values) {
        ByteArrayPool.getInstance().release(dataArray);
      }
      values.clear();
    }
  }

  public synchronized int size() {
    return dataSize;
  }

  public void close() {
    reset();
  }


  public synchronized void writeTo(OutputStream out) throws IOException {
    int lastArrayIndex = dataSize / ARRAY_SIZE;
    int lastElementIndex = dataSize % ARRAY_SIZE;

    for (int arrayIndex = 0; arrayIndex < lastArrayIndex; arrayIndex++) {
      out.write(values.get(arrayIndex), 0, ARRAY_SIZE);
    }

    if (lastElementIndex != 0)
      out.write(values.get(lastArrayIndex), 0, lastElementIndex);
  }

  public synchronized byte[] toByteArray() {
    byte[] buf = new byte[dataSize];
    int lastArrayIndex = dataSize / ARRAY_SIZE;
    int lastElementIndex = dataSize % ARRAY_SIZE;

    int bufIndex = 0;
    for (int arrayIndex = 0; arrayIndex < lastArrayIndex; arrayIndex++) {
      System.arraycopy(values.get(arrayIndex), 0, buf, bufIndex, ARRAY_SIZE);
      bufIndex += ARRAY_SIZE;
    }

    if (lastElementIndex != 0)
      System.arraycopy(values.get(lastArrayIndex), 0, buf, bufIndex, lastElementIndex);

    return buf;

  }

  /**
   * We are not sure whether following functions will be invoked in PublicBOAS. We'd defer to
   * implement them, but throw exception to avoid unexpected calling.
   */
  public byte[] getBuf() {
    throw new UnsupportedOperationException();
  }

  public synchronized String toString() {
    throw new UnsupportedOperationException();
  }

  public synchronized String toString(String charsetName) {
    throw new UnsupportedOperationException();
  }


}

