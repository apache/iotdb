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

package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * This class is just for reading batch data reversely. The data source is from page reader. For
 * example, the timeRet from pageReader is [1, 1000], It will be written in ascending sequence, but
 * the sequence of reading will be 1000 -> 1.
 */
public class DescReadBatchData extends BatchData {

  public DescReadBatchData() {
    batchDataType = BatchDataType.DescRead;
  }

  public DescReadBatchData(TSDataType dataType) {
    super(dataType);
    batchDataType = BatchDataType.DescRead;
  }

  @Override
  public boolean hasCurrent() {
    return super.readCurListIndex >= 0 && super.readCurArrayIndex >= 0;
  }

  @Override
  public void next() {
    super.readCurArrayIndex--;
    if (super.readCurArrayIndex == -1) {
      super.readCurArrayIndex = capacity - 1;
      super.readCurListIndex--;
    }
  }

  @Override
  public void resetBatchData() {
    super.readCurArrayIndex = writeCurArrayIndex - 1;
    super.readCurListIndex = writeCurListIndex;
  }

  /**
   * When put data, the writeIndex increases while the readIndex remains 0. For descending read, we
   * need to read from writeIndex to 0 (set the readIndex to writeIndex)
   */
  @Override
  public BatchData flip() {
    super.readCurArrayIndex = writeCurArrayIndex - 1;
    super.readCurListIndex = writeCurListIndex;
    return this;
  }

  @Override
  public Object getValueInTimestamp(long time) {
    while (hasCurrent()) {
      if (currentTime() > time) {
        next();
      } else if (currentTime() == time) {
        Object value = currentValue();
        next();
        return value;
      } else {
        return null;
      }
    }
    return null;
  }
}
