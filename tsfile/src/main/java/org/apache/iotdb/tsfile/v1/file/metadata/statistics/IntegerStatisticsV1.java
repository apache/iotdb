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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * Statistics for int type.
 */
public class IntegerStatisticsV1 extends StatisticsV1<Integer> {

  private int min;
  private int max;
  private int first;
  private int last;
  private double sum;

  @Override
  public Integer getMin() {
    return min;
  }

  @Override
  public Integer getMax() {
    return max;
  }

  @Override
  public Integer getFirst() {
    return first;
  }

  @Override
  public Integer getLast() {
    return last;
  }

  @Override
  public double getSum() {
    return sum;
  }

  @Override
  void deserialize(ByteBuffer byteBuffer) throws IOException {
    this.min = ReadWriteIOUtils.readInt(byteBuffer);
    this.max = ReadWriteIOUtils.readInt(byteBuffer);
    this.first = ReadWriteIOUtils.readInt(byteBuffer);
    this.last = ReadWriteIOUtils.readInt(byteBuffer);
    this.sum = ReadWriteIOUtils.readDouble(byteBuffer);
  }

  @Override
  void deserialize(InputStream inputStream) throws IOException {
    this.min = ReadWriteIOUtils.readInt(inputStream);
    this.max = ReadWriteIOUtils.readInt(inputStream);
    this.first = ReadWriteIOUtils.readInt(inputStream);
    this.last = ReadWriteIOUtils.readInt(inputStream);
    this.sum = ReadWriteIOUtils.readDouble(inputStream);
  }
}
