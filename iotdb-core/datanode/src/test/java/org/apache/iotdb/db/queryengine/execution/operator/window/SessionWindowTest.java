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
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;

public class SessionWindowTest {

  @Test
  public void testSatisfyRejectsDistanceBeyondLongMax() {
    SessionWindow sessionWindow = new SessionWindow(Long.MAX_VALUE, true);
    Column firstColumn = Mockito.mock(Column.class);
    Mockito.when(firstColumn.getLong(0)).thenReturn(Long.MIN_VALUE);
    sessionWindow.mergeOnePoint(new Column[] {firstColumn}, 0);

    Column secondColumn = Mockito.mock(Column.class);
    Mockito.when(secondColumn.getLong(0)).thenReturn(Long.MAX_VALUE);

    assertFalse(sessionWindow.satisfy(secondColumn, 0));
  }

  @Test
  public void testContainsRejectsRangeBeyondLongMax() {
    SessionWindow sessionWindow = new SessionWindow(Long.MAX_VALUE, true);
    Column column = Mockito.mock(Column.class);
    Mockito.when(column.getPositionCount()).thenReturn(2);
    Mockito.when(column.getLong(0)).thenReturn(Long.MIN_VALUE);
    Mockito.when(column.getLong(1)).thenReturn(Long.MAX_VALUE);

    assertFalse(sessionWindow.contains(column));
  }
}
