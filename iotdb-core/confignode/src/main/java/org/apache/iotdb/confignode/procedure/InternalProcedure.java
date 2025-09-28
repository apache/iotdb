1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure;
1
1import java.io.IOException;
1import java.nio.ByteBuffer;
1
1/**
1 * Internal Procedure, do some periodic job for framework.
1 *
1 * @param <Env>
1 */
1public abstract class InternalProcedure<Env> extends Procedure<Env> {
1  protected InternalProcedure(long toMillis) {
1    setTimeout(toMillis);
1  }
1
1  protected abstract void periodicExecute(final Env env);
1
1  @Override
1  protected Procedure<Env>[] execute(Env env) throws InterruptedException {
1    throw new UnsupportedOperationException();
1  }
1
1  @Override
1  protected void rollback(Env env) throws IOException, InterruptedException {
1    throw new UnsupportedOperationException();
1  }
1
1  @Override
1  public void deserialize(ByteBuffer byteBuffer) {}
1}
1