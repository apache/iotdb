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

package org.apache.iotdb.db.storageengine.rescon.quotas;

import org.apache.iotdb.commons.exception.RpcThrottlingException;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import java.nio.ByteBuffer;
import java.util.List;

public class ResourceAwareOperationQuota implements OperationQuota {

  private final OperationQuota delegate;
  private final QuotaTokenBundle resourceBundle;

  public ResourceAwareOperationQuota(OperationQuota delegate, QuotaTokenBundle resourceBundle) {
    this.delegate = delegate;
    this.resourceBundle = resourceBundle;
  }

  @Override
  public void checkQuota(int numWrites, int numReads, Statement s) throws RpcThrottlingException {
    delegate.checkQuota(numWrites, numReads, s);
  }

  @Override
  public void addReadResult(List<ByteBuffer> queryResult) {
    delegate.addReadResult(queryResult);
  }

  @Override
  public void close() {
    delegate.close();
    if (resourceBundle != null) {
      resourceBundle.close();
    }
  }
}
