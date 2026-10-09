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

package org.apache.iotdb.confignode.procedure.impl.schema.table;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.confignode.procedure.NoopProcedureStore;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.state.schema.DropTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DropTableProcedureTest {
  @Test
  public void dropTableDoesNotOccupyRegularProcedureWorker() throws Exception {
    final NoopProcedureStore store = new NoopProcedureStore();
    final ProcedureExecutor<ConfigNodeProcedureEnv> executor = new ProcedureExecutor<>(null, store);
    final CountDownLatch dropStarted = new CountDownLatch(1);
    final CountDownLatch releaseDrop = new CountDownLatch(1);
    final CountDownLatch regularCompleted = new CountDownLatch(1);
    store.start();
    executor.init(1);
    executor.startWorkers();
    try {
      executor.submitProcedureAndPersist(
          new DropTableProcedure("db", "table", "drop", false) {
            @Override
            protected Flow executeFromState(
                final ConfigNodeProcedureEnv env, final DropTableState state)
                throws InterruptedException {
              dropStarted.countDown();
              releaseDrop.await();
              return Flow.NO_MORE_STATE;
            }
          });
      Assert.assertTrue(dropStarted.await(10, TimeUnit.SECONDS));

      executor.submitProcedure(
          new Procedure<ConfigNodeProcedureEnv>() {
            @Override
            protected Procedure<ConfigNodeProcedureEnv>[] execute(
                final ConfigNodeProcedureEnv env) {
              regularCompleted.countDown();
              return null;
            }

            @Override
            protected void rollback(final ConfigNodeProcedureEnv env) {}
          });
      Assert.assertTrue(regularCompleted.await(10, TimeUnit.SECONDS));
    } finally {
      releaseDrop.countDown();
      executor.stop();
      executor.join();
      store.stop();
    }
  }

  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    final DropTableProcedure dropTableProcedure =
        new DropTableProcedure("database1", "table1", "0", false);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    dropTableProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(ProcedureType.DROP_TABLE_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final DropTableProcedure deserializedProcedure = new DropTableProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(dropTableProcedure, deserializedProcedure);
  }
}
