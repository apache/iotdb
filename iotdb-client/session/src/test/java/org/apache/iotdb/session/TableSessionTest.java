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

package org.apache.iotdb.session;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.service.rpc.thrift.TestInsertStage;

import org.apache.tsfile.write.record.Tablet;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TableSessionTest {
  @Test
  public void testInsertionStagesUseOnlyTheTableTestRpc() throws Exception {
    Session delegate = mock(Session.class);
    Tablet tablet = mock(Tablet.class);
    try (ITableSession session = new TableSession(delegate)) {
      session.testInsert(tablet);
      for (TestInsertStage stage : TestInsertStage.values()) {
        session.testInsert(tablet, stage);
      }
    }
    for (TestInsertStage stage : TestInsertStage.values()) {
      verify(delegate, times(stage == TestInsertStage.UNTIL_RPC_PROCESSED ? 2 : 1))
          .testInsertRelationalTablet(tablet, stage);
    }
    verify(delegate, never()).insertRelationalTablet(tablet);
    verify(delegate).close();
  }
}
