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

import org.apache.iotdb.commons.quota.OperationType;
import org.apache.iotdb.commons.quota.ResourceType;

public class QuotaToken implements AutoCloseable {

  private final UserResourceQuotaManager manager;
  private final String user;
  private final OperationType op;
  private final ResourceType resource;
  private final long amount;
  private boolean released;

  public QuotaToken(
      UserResourceQuotaManager manager,
      String user,
      OperationType op,
      ResourceType resource,
      long amount) {
    this.manager = manager;
    this.user = user;
    this.op = op;
    this.resource = resource;
    this.amount = amount;
  }

  public String getUser() {
    return user;
  }

  public OperationType getOp() {
    return op;
  }

  public ResourceType getResource() {
    return resource;
  }

  public long getAmount() {
    return amount;
  }

  @Override
  public void close() {
    if (!released) {
      released = true;
      manager.release(this);
    }
  }
}
