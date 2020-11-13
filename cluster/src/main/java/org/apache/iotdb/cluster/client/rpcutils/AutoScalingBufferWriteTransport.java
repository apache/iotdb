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

package org.apache.iotdb.cluster.client.rpcutils;

import org.apache.thrift.transport.AutoExpandingBuffer;
import org.apache.thrift.transport.TTransport;

/**
 * Note that this class is mainly copied from class {@link org.apache.thrift.transport.AutoExpandingBufferWriteTransport}.
 * since that class does not support inheritance, so rewrite this class.
 */
public class AutoScalingBufferWriteTransport extends TTransport {

  private final AutoScalingBuffer buf;
  private int pos;

  public AutoScalingBufferWriteTransport(int initialCapacity, double growthCoefficient) {
    this.buf = new AutoScalingBuffer(initialCapacity, growthCoefficient);
    this.pos = 0;
  }

  @Override
  public void close() {
  }

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public void open() {
  }

  @Override
  public int read(byte[] buf, int off, int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] toWrite, int off, int len) {
    buf.resizeIfNecessary(pos + len);
    System.arraycopy(toWrite, off, buf.array(), pos, len);
    pos += len;
  }

  public AutoExpandingBuffer getBuf() {
    return buf;
  }

  public int getPos() {
    return pos;
  }

  public void reset() {
    pos = 0;
  }

  /**
   * shrink the buffer to the specific size
   *
   * @param size The size of the target you want to shrink to
   */
  public void shrinkSizeIfNecessary(int size) {
    buf.shrinkSizeIfNecessary(size);
  }
}
