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

import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.utils.BitMap;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

public class DefaultOperationQuotaTest {

  @Test
  public void testCheckQuotaWithNullAndSparseBitMaps() throws Exception {
    final DefaultOperationQuota quota = new DefaultOperationQuota(createQuotaLimiter());

    final InsertTabletStatement tabletWithoutBitMaps = new InsertTabletStatement();
    tabletWithoutBitMaps.setBitMaps(null);
    quota.checkQuota(1, 0, tabletWithoutBitMaps);

    final InsertTabletStatement tabletWithSparseBitMaps = new InsertTabletStatement();
    final BitMap bitMap = new BitMap(8);
    bitMap.mark(0);
    tabletWithSparseBitMaps.setBitMaps(new BitMap[] {null, bitMap});
    quota.checkQuota(1, 0, tabletWithSparseBitMaps);

    final InsertMultiTabletsStatement multiTabletsStatement = new InsertMultiTabletsStatement();
    multiTabletsStatement.setInsertTabletStatementList(
        Arrays.asList(tabletWithoutBitMaps, tabletWithSparseBitMaps));
    quota.checkQuota(1, 0, multiTabletsStatement);
  }

  private static QuotaLimiter createQuotaLimiter() {
    final Map<ThrottleType, TTimedQuota> quotas = new EnumMap<>(ThrottleType.class);
    for (final ThrottleType throttleType : ThrottleType.values()) {
      quotas.put(throttleType, new TTimedQuota(60_000L, 1_000_000_000L));
    }
    return QuotaLimiter.fromThrottle(Collections.unmodifiableMap(quotas));
  }
}
