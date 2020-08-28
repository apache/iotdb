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

package org.apache.iotdb.db.query.udf.api.customizer.strategy;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.constant.DatetimeUtils.DurationUnit;

public class RowRecordSizeLimitedBatchIterationStrategy extends RowRecordBatchIterationStrategy {

  private final int batchSize;

  private Long displayWindowBegin;

  public RowRecordSizeLimitedBatchIterationStrategy(String tabletName, int batchSize) {
    super(tabletName);
    this.batchSize = batchSize;
  }

  public RowRecordSizeLimitedBatchIterationStrategy(String tabletName, int batchSize,
      long displayWindowBegin, DurationUnit displayWindowBeginTimeUnit) {
    super(tabletName);
    this.batchSize = batchSize;
    this.displayWindowBegin = DatetimeUtils
        .convertDurationStrToLong(displayWindowBegin, displayWindowBeginTimeUnit.toString(), "ns");
  }

  @Override
  public void check() throws QueryProcessException {
    if (batchSize <= 0) {
      throw new QueryProcessException(
          String.format("Parameter batchSize(%d) should be positive.", batchSize));
    }
  }

  public int getBatchSize() {
    return batchSize;
  }

  public boolean hasDisplayWindowBegin() {
    return displayWindowBegin != null;
  }

  public Long getDisplayWindowBegin() {
    return displayWindowBegin;
  }
}
