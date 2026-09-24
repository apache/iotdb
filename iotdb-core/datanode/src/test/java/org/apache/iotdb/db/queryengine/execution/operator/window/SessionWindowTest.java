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

package org.apache.iotdb.db.queryengine.execution.operator.window;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class SessionWindowTest {

  @Test
  public void satisfyHandlesOverflowedTimeDistance() {
    SessionWindow window = new SessionWindow(1, true);
    Column timeColumn = buildTimeColumn(Long.MIN_VALUE, Long.MAX_VALUE);
    window.mergeOnePoint(new Column[] {timeColumn}, 0);

    Assert.assertFalse(window.satisfy(timeColumn, 1));
  }

  @Test
  public void skipPointsOutOfCurWindowHandlesOverflowedTimeDistance() {
    SessionWindowManager manager = new SessionWindowManager(false, 1, true);
    manager.initCurWindow();
    Column previousTimeColumn = buildTimeColumn(Long.MIN_VALUE);
    manager.getCurWindow().mergeOnePoint(new Column[] {previousTimeColumn}, 0);
    manager.next();

    TsBlock nextBlock = buildTsBlock(Long.MAX_VALUE);
    TsBlock skippedBlock = manager.skipPointsOutOfCurWindow(nextBlock);

    Assert.assertEquals(1, skippedBlock.getPositionCount());
    Assert.assertEquals(Long.MAX_VALUE, skippedBlock.getTimeColumn().getLong(0));
  }

  private Column buildTimeColumn(long... timestamps) {
    return buildTsBlock(timestamps).getTimeColumn();
  }

  private TsBlock buildTsBlock(long... timestamps) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    for (long timestamp : timestamps) {
      builder.getTimeColumnBuilder().writeLong(timestamp);
      builder.getColumnBuilder(0).appendNull();
      builder.declarePosition();
    }
    return builder.build();
  }
}
