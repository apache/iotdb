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

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Internal Procedure, do some preiodic job for framework
 *
 * @param <Env>
 */
public abstract class InternalProcedure<Env> extends Procedure<Env> {
  protected InternalProcedure(long toMillis) {
    setTimeout(toMillis);
  }

  protected abstract void periodicExecute(final Env env);

  @Override
  protected Procedure<Env>[] execute(Env env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void rollback(Env env) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean abort(Env env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {}
}
