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

package org.apache.iotdb.db.mpp.sql.rewriter;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.mpp.sql.statement.component.ResultColumn;

import java.util.List;

/** apply MaxQueryDeduplicatedPathNum and SLIMIT & SOFFSET */
public class ColumnPaginationController {

  // series offset for result set. The default value is 0
  private final int seriesOffset;

  private int curLimit =
      IoTDBDescriptor.getInstance().getConfig().getMaxQueryDeduplicatedPathNum() + 1;
  private int curOffset;

  // records the path number that the SchemaTree totally returned
  private int consumed = 0;

  public ColumnPaginationController(int seriesLimit, int seriesOffset) {
    // for series limit, the default value is 0, which means no limit
    this.curLimit = seriesLimit == 0 ? this.curLimit : Math.min(seriesLimit, this.curLimit);
    this.seriesOffset = this.curOffset = seriesOffset;
  }

  public int getCurLimit() {
    return curLimit;
  }

  public int getCurOffset() {
    return curOffset;
  }

  /** @return should break the loop or not */
  public boolean checkIfPathNumberIsOverLimit(List<ResultColumn> resultColumns)
      throws PathNumOverLimitException {
    if (resultColumns.size()
        > IoTDBDescriptor.getInstance().getConfig().getMaxQueryDeduplicatedPathNum()) {
      throw new PathNumOverLimitException();
    }
    return curLimit == 0;
  }

  public void checkIfSoffsetIsExceeded(List<ResultColumn> resultColumns)
      throws StatementAnalyzeException {
    if (consumed == 0 ? seriesOffset != 0 : resultColumns.isEmpty()) {
      throw new StatementAnalyzeException(
          String.format(
              "The value of SOFFSET (%d) is equal to or exceeds the number of sequences (%d) that can actually be returned.",
              seriesOffset, consumed));
    }
  }

  public void consume(int limit, int offset) {
    consumed += offset;
    curOffset -= Math.min(curOffset, offset);
    curLimit -= limit;
  }

  public boolean hasCurOffset() {
    return curOffset != 0;
  }

  public boolean hasCurLimit() {
    return curLimit != 0;
  }

  public void decCurOffset() {
    curOffset--;
  }

  public void decCurLimit() {
    curLimit--;
  }

  public void incConsumed(int num) {
    consumed += num;
  }
}
