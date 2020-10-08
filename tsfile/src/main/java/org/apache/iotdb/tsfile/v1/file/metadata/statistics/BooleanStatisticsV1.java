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
 * Boolean Statistics.
 */
public class BooleanStatisticsV1 extends StatisticsV1<Boolean> {

  private boolean min;
  private boolean max;
  private boolean first;
  private boolean last;
  private double sum;

  @Override
  public Boolean getMin() {
    return min;
  }

  @Override
  public Boolean getMax() {
    return max;
  }

  @Override
  public Boolean getFirst() {
    return first;
  }

  @Override
  public Boolean getLast() {
    return last;
  }

  @Override
  public double getSum() {
    return sum;
  }

  @Override
  void deserialize(ByteBuffer byteBuffer) throws IOException {
    this.min = ReadWriteIOUtils.readBool(byteBuffer);
    this.max = ReadWriteIOUtils.readBool(byteBuffer);
    this.first = ReadWriteIOUtils.readBool(byteBuffer);
    this.last = ReadWriteIOUtils.readBool(byteBuffer);
    this.sum = ReadWriteIOUtils.readDouble(byteBuffer);
  }

  @Override
  void deserialize(InputStream inputStream) throws IOException {
    this.min = ReadWriteIOUtils.readBool(inputStream);
    this.max = ReadWriteIOUtils.readBool(inputStream);
    this.first = ReadWriteIOUtils.readBool(inputStream);
    this.last = ReadWriteIOUtils.readBool(inputStream);
    this.sum = ReadWriteIOUtils.readDouble(inputStream);
  }

}
