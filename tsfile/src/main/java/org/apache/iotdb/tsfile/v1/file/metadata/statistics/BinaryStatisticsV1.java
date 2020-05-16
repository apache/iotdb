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
package org.apache.iotdb.tsfile.v1.file.metadata.statistics;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * Statistics for string type.
 */
public class BinaryStatisticsV1 extends StatisticsV1<Binary> {

  private Binary min = new Binary("");
  private Binary max = new Binary("");
  private Binary first = new Binary("");
  private Binary last = new Binary("");
  private double sum;

  @Override
  public Binary getMin() {
    return min;
  }

  @Override
  public Binary getMax() {
    return max;
  }

  @Override
  public Binary getFirst() {
    return first;
  }

  @Override
  public Binary getLast() {
    return last;
  }

  @Override
  public double getSum() {
    return sum;
  }

  @Override
  void deserialize(ByteBuffer byteBuffer) throws IOException {
    this.min = new Binary(
        ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(byteBuffer).array());
    this.max = new Binary(
        ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(byteBuffer).array());
    this.first = new Binary(
        ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(byteBuffer).array());
    this.last = new Binary(
        ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(byteBuffer).array());
    this.sum = ReadWriteIOUtils.readDouble(byteBuffer);
  }

  @Override
  void deserialize(InputStream inputStream) throws IOException {
    this.min = new Binary(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream));
    this.max = new Binary(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream));
    this.first = new Binary(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream));
    this.last = new Binary(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream));
    this.sum = ReadWriteIOUtils.readDouble(inputStream);
  }
}
