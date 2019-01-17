/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.query.fill;

import java.io.IOException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;

public class LinearFill extends IFill {

  private long beforeRange;
  private long afterRange;

  private Path path;

  private BatchData result;

  public LinearFill(long beforeRange, long afterRange) {
    this.beforeRange = beforeRange;
    this.afterRange = afterRange;
  }

  /**
   * Constructor of LinearFill.
   */
  public LinearFill(Path path, TSDataType dataType, long queryTime, long beforeRange,
      long afterRange) {
    super(dataType, queryTime);
    this.path = path;
    this.beforeRange = beforeRange;
    this.afterRange = afterRange;
    result = new BatchData(dataType, true, true);
  }

  public long getBeforeRange() {
    return beforeRange;
  }

  public void setBeforeRange(long beforeRange) {
    this.beforeRange = beforeRange;
  }

  public long getAfterRange() {
    return afterRange;
  }

  public void setAfterRange(long afterRange) {
    this.afterRange = afterRange;
  }

  @Override
  public IFill copy(Path path) {
    return new LinearFill(path, dataType, queryTime, beforeRange, afterRange);
  }

  @Override
  public BatchData getFillResult() throws ProcessorException, IOException, PathErrorException {
    long beforeTime;
    if (beforeRange == -1) {
      beforeTime = 0;
    } else {
      beforeTime = queryTime - beforeRange;
    }
    long afterTime;
    if (afterRange == -1) {
      afterTime = Long.MAX_VALUE;
    } else {
      afterTime = queryTime + afterRange;
    }

    return result;
  }
}