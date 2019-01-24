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
package org.apache.iotdb.tsfile.file.header;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.NoStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class PageHeader {

  private int uncompressedSize;
  private int compressedSize;
  private int numOfValues;
  private Statistics<?> statistics;
  private long maxTimestamp;
  private long minTimestamp;

  // this field does not need to be serialized.
  private int serializedSize;

  public PageHeader(int uncompressedSize, int compressedSize, int numOfValues,
                    Statistics<?> statistics,
                    long maxTimestamp, long minTimestamp) {
    this.uncompressedSize = uncompressedSize;
    this.compressedSize = compressedSize;
    this.numOfValues = numOfValues;
    if (statistics == null) {
      statistics = new NoStatistics();
    }
    this.statistics = statistics;
    this.maxTimestamp = maxTimestamp;
    this.minTimestamp = minTimestamp;
    serializedSize = calculatePageHeaderSize();
  }

  public static int calculatePageHeaderSize(TSDataType type) {
    return 3 * Integer.BYTES + 2 * Long.BYTES + Statistics.getStatsByType(type).getSerializedSize();
  }

  public static PageHeader deserializeFrom(InputStream inputStream, TSDataType dataType)
      throws IOException {
    int uncompressedSize = ReadWriteIOUtils.readInt(inputStream);
    int compressedSize = ReadWriteIOUtils.readInt(inputStream);
    int numOfValues = ReadWriteIOUtils.readInt(inputStream);
    long maxTimestamp = ReadWriteIOUtils.readLong(inputStream);
    long minTimestamp = ReadWriteIOUtils.readLong(inputStream);
    Statistics statistics = Statistics.deserialize(inputStream, dataType);
    return new PageHeader(uncompressedSize, compressedSize, numOfValues, statistics, maxTimestamp,
        minTimestamp);
  }

  public static PageHeader deserializeFrom(ByteBuffer buffer, TSDataType dataType)
      throws IOException {
    int uncompressedSize = ReadWriteIOUtils.readInt(buffer);
    int compressedSize = ReadWriteIOUtils.readInt(buffer);
    int numOfValues = ReadWriteIOUtils.readInt(buffer);
    long maxTimestamp = ReadWriteIOUtils.readLong(buffer);
    long minTimestamp = ReadWriteIOUtils.readLong(buffer);
    Statistics statistics = Statistics.deserialize(buffer, dataType);
    return new PageHeader(uncompressedSize, compressedSize, numOfValues, statistics, maxTimestamp,
        minTimestamp);
  }

  public int calculatePageHeaderSize() {
    return 3 * Integer.BYTES + 2 * Long.BYTES + statistics.getSerializedSize();
  }

  public int getSerializedSize() {
    return serializedSize;
  }

  public int getUncompressedSize() {
    return uncompressedSize;
  }

  public void setUncompressedSize(int uncompressedSize) {
    this.uncompressedSize = uncompressedSize;
  }

  public int getCompressedSize() {
    return compressedSize;
  }

  public void setCompressedSize(int compressedSize) {
    this.compressedSize = compressedSize;
  }

  public int getNumOfValues() {
    return numOfValues;
  }

  public void setNumOfValues(int numOfValues) {
    this.numOfValues = numOfValues;
  }

  public Statistics<?> getStatistics() {
    return statistics;
  }

  public long getMaxTimestamp() {
    return maxTimestamp;
  }

  public void setMaxTimestamp(long maxTimestamp) {
    this.maxTimestamp = maxTimestamp;
  }

  public long getMinTimestamp() {
    return minTimestamp;
  }

  public void setMinTimestamp(long minTimestamp) {
    this.minTimestamp = minTimestamp;
  }

  public int serializeTo(OutputStream outputStream) throws IOException {
    int length = 0;
    length += ReadWriteIOUtils.write(uncompressedSize, outputStream);
    length += ReadWriteIOUtils.write(compressedSize, outputStream);
    length += ReadWriteIOUtils.write(numOfValues, outputStream);
    length += ReadWriteIOUtils.write(maxTimestamp, outputStream);
    length += ReadWriteIOUtils.write(minTimestamp, outputStream);
    length += statistics.serialize(outputStream);
    assert length == getSerializedSize();
    return length;
  }

  @Override
  public String toString() {
    return "PageHeader{" + "uncompressedSize=" + uncompressedSize + ", compressedSize="
        + compressedSize
        + ", numOfValues=" + numOfValues + ", statistics=" + statistics + ", maxTimestamp="
        + maxTimestamp
        + ", minTimestamp=" + minTimestamp + ", serializedSize=" + serializedSize + '}';
  }
}
