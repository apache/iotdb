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

package org.apache.iotdb.confignode.procedure.entity;

import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class StuckProcedure extends Procedure<TestProcEnv> {
  private Semaphore latch;

  public StuckProcedure() {}

  public StuckProcedure(final Semaphore latch) {
    this.latch = latch;
  }

  @Override
  protected Procedure[] execute(final TestProcEnv env) {
    try {
      if (!latch.tryAcquire(1, 30, TimeUnit.SECONDS)) {
        throw new Exception("waited too long");
      }

      if (!latch.tryAcquire(1, 30, TimeUnit.SECONDS)) {
        throw new Exception("waited too long");
      }
    } catch (Exception e) {
      setFailure("StuckProcedure", e);
    }
    return null;
  }

  @Override
  protected void rollback(TestProcEnv testProcEnv) throws IOException, InterruptedException {}

  @Override
  protected boolean abort(TestProcEnv testProcEnv) {
    return false;
  }
}
