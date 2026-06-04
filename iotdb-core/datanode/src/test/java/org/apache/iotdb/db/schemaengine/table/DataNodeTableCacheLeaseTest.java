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

package org.apache.iotdb.db.schemaengine.table;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DataNodeTableCacheLeaseTest {

  @After
  public void renewLease() {
    // Renew so a forced-fenced lease does not leak into other tests sharing this JVM fork.
    MetadataLeaseManager.getInstance().recordConfigNodeHeartbeat();
  }

  @Test
  public void getTableInWriteFailsClosedWhenLeaseFenced() {
    MetadataLeaseManager.getInstance().expireLeaseForTest();
    try {
      DataNodeTableCache.getInstance().getTableInWrite("root.db", "t");
      fail("expected fail-closed retry exception while the metadata lease is fenced");
    } catch (final IoTDBRuntimeException e) {
      // A fenced DataNode must refuse to validate writes against a possibly-stale cache, and the
      // error must be the retryable one (not, say, table-not-exists from the stale cache).
      assertEquals(TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode(), e.getErrorCode());
    }
  }

  @Test
  public void getTableFailsClosedWhenLeaseFenced() {
    MetadataLeaseManager.getInstance().expireLeaseForTest();
    try {
      DataNodeTableCache.getInstance().getTable("root.db", "t");
      fail("expected fail-closed retry exception while the metadata lease is fenced");
    } catch (final IoTDBRuntimeException e) {
      assertEquals(TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode(), e.getErrorCode());
    }
  }
}
