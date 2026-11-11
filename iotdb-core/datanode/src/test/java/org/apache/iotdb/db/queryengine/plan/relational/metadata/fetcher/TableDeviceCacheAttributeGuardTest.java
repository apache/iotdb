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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.db.schemaengine.schemaregion.attribute.update.UpdateDetailContainer;
import org.apache.iotdb.mpp.rpc.thrift.TAttributeUpdateReq;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaRegionAttributeInfo;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableDeviceCacheAttributeGuardTest {
  @Test
  public void testSchemaFetchPreservesAcceptedVersion() {
    final TableDeviceCacheAttributeGuard guard = new TableDeviceCacheAttributeGuard();
    guard.addFetchedRegion(1);
    final TAttributeUpdateReq accepted = update(2);
    guard.handleAttributeUpdate(accepted);
    assertFalse(accepted.getAttributeUpdateMap().isEmpty());

    guard.addFetchedRegion(1);
    for (final long version : new long[] {1, 2}) {
      final TAttributeUpdateReq stale = update(version);
      guard.handleAttributeUpdate(stale);
      assertTrue(stale.getAttributeUpdateMap().isEmpty());
    }
    final TAttributeUpdateReq newer = update(3);
    guard.handleAttributeUpdate(newer);
    assertFalse(newer.getAttributeUpdateMap().isEmpty());
  }

  private static TAttributeUpdateReq update(final long version) {
    final byte[] body =
        new UpdateDetailContainer().getUpdateContent(new AtomicInteger(1024), new AtomicBoolean());
    final Map<Integer, TSchemaRegionAttributeInfo> updates = new HashMap<>();
    updates.put(1, new TSchemaRegionAttributeInfo(version, "db", ByteBuffer.wrap(body)));
    return new TAttributeUpdateReq(updates);
  }
}
