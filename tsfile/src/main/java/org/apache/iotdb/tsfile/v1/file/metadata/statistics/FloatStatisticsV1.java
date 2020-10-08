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
 * Statistics for float type.
 */
public class FloatStatisticsV1 extends StatisticsV1<Float> {

  private float min;
  private float max;
  private float first;
  private double sum;
  private float last;

  @Override
  public Float getMin() {
    return min;
  }

  @Override
  public Float getMax() {
    return max;
  }

  @Override
  public Float getFirst() {
    return first;
  }

  @Override
  public Float getLast() {
    return last;
  }

  @Override
  public double getSum() {
    return sum;
  }

  @Override
  void deserialize(ByteBuffer byteBuffer) throws IOException {
    this.min = ReadWriteIOUtils.readFloat(byteBuffer);
    this.max = ReadWriteIOUtils.readFloat(byteBuffer);
    this.first = ReadWriteIOUtils.readFloat(byteBuffer);
    this.last = ReadWriteIOUtils.readFloat(byteBuffer);
    this.sum = ReadWriteIOUtils.readDouble(byteBuffer);
  }

  @Override
  void deserialize(InputStream inputStream) throws IOException {
    this.min = ReadWriteIOUtils.readFloat(inputStream);
    this.max = ReadWriteIOUtils.readFloat(inputStream);
    this.first = ReadWriteIOUtils.readFloat(inputStream);
    this.last = ReadWriteIOUtils.readFloat(inputStream);
    this.sum = ReadWriteIOUtils.readDouble(inputStream);
  }
}
