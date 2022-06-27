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
import org.apache.iotdb.confignode.procedure.store.IProcedureFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TestProcedureFactory implements IProcedureFactory {

  @Override
  public Procedure create(ByteBuffer buffer) throws IOException {
    int typeNum = buffer.getInt();
    if (typeNum >= TestProcedureType.values().length) {
      throw new IOException("unrecognized log type " + typeNum);
    }
    TestProcedureType type = TestProcedureType.values()[typeNum];
    Procedure procedure;
    switch (type) {
      case INC_PROCEDURE:
        procedure = new IncProcedure();
        break;
      case NOOP_PROCEDURE:
        procedure = new NoopProcedure();
        break;
      case SLEEP_PROCEDURE:
        procedure = new SleepProcedure();
        break;
      case STUCK_PROCEDURE:
        procedure = new StuckProcedure();
        break;
      case STUCK_STM_PROCEDURE:
        procedure = new StuckSTMProcedure();
        break;
      case SIMPLE_STM_PROCEDURE:
        procedure = new SimpleSTMProcedure();
        break;
      case SIMPLE_LOCK_PROCEDURE:
        procedure = new SimpleLockProcedure();
        break;
      default:
        throw new IOException("unknown Procedure type: " + typeNum);
    }
    procedure.deserialize(buffer);
    return procedure;
  }

  public enum TestProcedureType {
    INC_PROCEDURE,
    NOOP_PROCEDURE,
    SIMPLE_LOCK_PROCEDURE,
    SIMPLE_STM_PROCEDURE,
    SLEEP_PROCEDURE,
    STUCK_PROCEDURE,
    STUCK_STM_PROCEDURE;
  }
}
