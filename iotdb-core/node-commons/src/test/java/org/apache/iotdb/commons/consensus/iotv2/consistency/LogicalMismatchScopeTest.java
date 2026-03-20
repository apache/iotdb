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

package org.apache.iotdb.commons.consensus.iotv2.consistency;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LogicalMismatchScopeTest {

  @Test
  public void shouldRoundTripLegacyLeafScope() {
    List<LogicalMismatchScope.Scope> scopes =
        LogicalMismatchScope.deserialize("LIVE@leaf:1:0,TOMBSTONE@leaf:2:0");

    Assert.assertEquals(
        Arrays.asList(
            new LogicalMismatchScope.Scope("LIVE", "leaf:1:0"),
            new LogicalMismatchScope.Scope("TOMBSTONE", "leaf:2:0")),
        scopes);
    Assert.assertEquals("LIVE@leaf:1:0,TOMBSTONE@leaf:2:0", LogicalMismatchScope.serialize(scopes));
  }

  @Test
  public void shouldPersistOptionalLogicalKeyRange() {
    List<LogicalMismatchScope.Scope> scopes =
        Collections.singletonList(
            new LogicalMismatchScope.Scope(
                "LIVE", "leaf:3:0", "root.db.d1|s1|1|INT64|1", "root.db.d1|s1|9|INT64|9"));

    String serialized = LogicalMismatchScope.serialize(scopes);
    List<LogicalMismatchScope.Scope> recovered = LogicalMismatchScope.deserialize(serialized);

    Assert.assertEquals(scopes, recovered);
  }

  @Test
  public void shouldPersistExactLogicalKeysForMicroRepair() {
    List<LogicalMismatchScope.Scope> scopes =
        Collections.singletonList(
            new LogicalMismatchScope.Scope(
                "LIVE",
                "leaf:7:0",
                "root.db.d1|s1|1|INT64|1",
                "root.db.d1|s1|5|INT64|5",
                Arrays.asList(
                    "root.db.d1|s1|1|INT64|1",
                    "root.db.d1|s2|2|INT64|2",
                    "root.db.d2|s1|5|INT64|5")));

    String serialized = LogicalMismatchScope.serialize(scopes);
    List<LogicalMismatchScope.Scope> recovered = LogicalMismatchScope.deserialize(serialized);

    Assert.assertEquals(scopes, recovered);
  }

  @Test
  public void shouldPersistNonRepairableDirective() {
    List<LogicalMismatchScope.Scope> scopes =
        Collections.singletonList(
            new LogicalMismatchScope.Scope(
                "TOMBSTONE",
                "leaf:9:0",
                "root.db.d1|s1|1|INT64|1",
                "root.db.d1|s1|5|INT64|5",
                Collections.singletonList("root.db.d1|s1|1|INT64|1"),
                LogicalMismatchScope.RepairDirective.FOLLOWER_EXTRA_TOMBSTONE));

    String serialized = LogicalMismatchScope.serialize(scopes);
    List<LogicalMismatchScope.Scope> recovered = LogicalMismatchScope.deserialize(serialized);

    Assert.assertEquals(scopes, recovered);
    Assert.assertFalse(recovered.get(0).isRepairable());
    Assert.assertEquals(
        LogicalMismatchScope.RepairDirective.FOLLOWER_EXTRA_TOMBSTONE,
        recovered.get(0).getRepairDirective());
  }
}
