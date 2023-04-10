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
package org.apache.iotdb.db.quotas;

import org.apache.iotdb.db.mpp.plan.statement.Statement;

import java.nio.ByteBuffer;
import java.util.List;

/** Noop operation quota returned when no quota is associated to the user/table */
class NoopOperationQuota implements OperationQuota {
  private static OperationQuota instance = new NoopOperationQuota();

  private NoopOperationQuota() {
    // no-op
  }

  public static OperationQuota get() {
    return instance;
  }

  @Override
  public void checkQuota(int numWrites, int numReads, Statement s) {}

  @Override
  public void addReadResult(List<ByteBuffer> queryResult) {}

  @Override
  public void close() {
    // no-op
  }
}
