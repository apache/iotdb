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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.PathNumOverLimitException;

/** apply MaxQueryDeduplicatedPathNum and SLIMIT & SOFFSET */
public class ColumnPaginationController {

  private int curLimit =
      IoTDBDescriptor.getInstance().getConfig().getMaxQueryDeduplicatedPathNum() + 1;
  private int curOffset;

  // records the path number that the SchemaProcessor totally returned
  private int consumed = 0;

  // for ALIGN BY DEVICE / DISABLE ALIGN / GROUP BY LEVEL / LAST, controller does is disabled
  private final boolean isDisabled;

  public ColumnPaginationController(int seriesLimit, int seriesOffset, boolean isDisabled) {
    // for series limit, the default value is 0, which means no limit
    this.curLimit = seriesLimit == 0 ? this.curLimit : Math.min(seriesLimit, this.curLimit);
    // series offset for result set. The default value is 0
    this.curOffset = seriesOffset;
    this.isDisabled = isDisabled;
  }

  public boolean hasCurOffset() {
    if (isDisabled) {
      return false;
    }

    return curOffset != 0;
  }

  public boolean hasCurLimit() {
    if (isDisabled) {
      return true;
    }

    return curLimit != 0;
  }

  public void consumeOffset() {
    if (isDisabled) {
      return;
    }

    curOffset--;
  }

  public void consumeLimit() {
    consumed++;
    if (consumed > IoTDBDescriptor.getInstance().getConfig().getMaxQueryDeduplicatedPathNum()) {
      throw new PathNumOverLimitException();
    }

    if (isDisabled) {
      return;
    }
    curLimit--;
  }
}
