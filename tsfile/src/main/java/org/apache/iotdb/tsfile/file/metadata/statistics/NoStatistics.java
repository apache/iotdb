/**
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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * This statistic is used as Unsupported data type. It just return a 0-byte array while asked max or
 * min.
 *
 * @author kangrong
 */
public class NoStatistics extends Statistics<Long> {

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    // NoStatistics does not make any statistics
  }

  @Override
  public Long getMin() {
    return null;
  }

  @Override
  public Long getMax() {
    return null;
  }

  @Override
  public void updateStats(boolean value) {
    // NoStatistics does not make any statistics
  }

  @Override
  public void updateStats(int value) {
    // NoStatistics does not make any statistics
  }

  @Override
  public void updateStats(long value) {
    // NoStatistics does not make any statistics
  }

  @Override
  public void updateStats(Binary value) {
    // NoStatistics does not make any statistics
  }

  @Override
  public void updateStats(long min, long max) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void mergeStatisticsValue(Statistics<?> stats) {
    // NoStatistics does not make any statistics
  }

  @Override
  public byte[] getMaxBytes() {
    return new byte[0];
  }

  @Override
  public byte[] getMinBytes() {
    return new byte[0];
  }

  @Override
  public String toString() {
    return "no stats";
  }

  @Override
  public Long getFirst() {
    return null;
  }

  @Override
  public double getSum() {
    return 0;
  }

  @Override
  public Long getLast() {
    return null;
  }

  @Override
  public byte[] getFirstBytes() {
    return new byte[0];
  }

  @Override
  public byte[] getSumBytes() {
    return new byte[0];
  }

  @Override
  public byte[] getLastBytes() {
    return new byte[0];
  }

  @Override
  public int sizeOfDatum() {
    return 0;
  }

  @Override
  public ByteBuffer getMaxBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(0);
  }

  @Override
  public ByteBuffer getMinBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(0);
  }

  @Override
  public ByteBuffer getFirstBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(0);
  }

  @Override
  public ByteBuffer getSumBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(0);
  }

  @Override
  public ByteBuffer getLastBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(0);
  }

  @Override
  void fill(InputStream inputStream) {
    // NoStatistics does not make any statistics
  }

  @Override
  void fill(ByteBuffer byteBuffer) {
    // NoStatistics does not make any statistics
  }
}
