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

public class QuotaTokenBundle implements AutoCloseable {

  private final QuotaToken cpuToken;
  private final QuotaToken memoryToken;
  private final QuotaToken tempDiskToken;

  public QuotaTokenBundle(QuotaToken cpuToken, QuotaToken memoryToken) {
    this(cpuToken, memoryToken, null);
  }

  public QuotaTokenBundle(QuotaToken cpuToken, QuotaToken memoryToken, QuotaToken tempDiskToken) {
    this.cpuToken = cpuToken;
    this.memoryToken = memoryToken;
    this.tempDiskToken = tempDiskToken;
  }

  @Override
  public void close() {
    if (tempDiskToken != null) {
      tempDiskToken.close();
    }
    if (memoryToken != null) {
      memoryToken.close();
    }
    if (cpuToken != null) {
      cpuToken.close();
    }
  }
}
