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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.confignode.manager.pipe.source;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.persistence.schema.CNSnapshotFileType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PipeConfigTreePrivilegeParseVisitorTest {

  @Test
  public void testPartialTTLAuthorizationProducesSeparatePlans() throws Exception {
    final SetTTLPlan originalPlan = new SetTTLPlan(100, "root", "db");
    originalPlan.setDataBase(true);
    final List<ConfigPhysicalPlan> plans =
        PipeConfigTreePrivilegeParseVisitor.createTTLPlans(
            Arrays.asList(
                new PartialPath("root.db.device1.**"), new PartialPath("root.db.device2.**")),
            originalPlan,
            new PartialPath("root.db"),
            new PartialPath("root.db.**"));

    Assert.assertEquals(2, plans.size());
    Assert.assertEquals("root.db.device1.**", pathOf((SetTTLPlan) plans.get(0)));
    Assert.assertEquals("root.db.device2.**", pathOf((SetTTLPlan) plans.get(1)));
    Assert.assertFalse(((SetTTLPlan) plans.get(0)).isDataBase());
    Assert.assertFalse(((SetTTLPlan) plans.get(1)).isDataBase());
  }

  @Test
  public void testTTLAndInvalidSnapshotsRequirePlanLevelChecks() {
    final PipeConfigTreePrivilegeParseVisitor visitor =
        new PipeConfigTreePrivilegeParseVisitor(true);
    Assert.assertFalse(visitor.canSkipSnapshotPrivilegeCheck(CNSnapshotFileType.TTL, "root"));
    Assert.assertFalse(visitor.canSkipSnapshotPrivilegeCheck(CNSnapshotFileType.INVALID, "root"));
  }

  @Test
  public void testAdminCanSkipSchemaSnapshotPrivilegeChecks() {
    final PipeConfigTreePrivilegeParseVisitor visitor =
        new PipeConfigTreePrivilegeParseVisitor(true);
    Assert.assertTrue(visitor.canSkipSnapshotPrivilegeCheck(CNSnapshotFileType.SCHEMA, "root"));
  }

  @Test
  public void testFullyAuthorizedDatabaseTTLRemainsDatabasePlan() throws Exception {
    final SetTTLPlan originalPlan = new SetTTLPlan(100, "root", "db");
    originalPlan.setDataBase(true);
    final List<ConfigPhysicalPlan> plans =
        PipeConfigTreePrivilegeParseVisitor.createTTLPlans(
            Collections.singletonList(new PartialPath("root.db.**")),
            originalPlan,
            new PartialPath("root.db"),
            new PartialPath("root.db.**"));

    Assert.assertEquals(1, plans.size());
    Assert.assertEquals("root.db", pathOf((SetTTLPlan) plans.get(0)));
    Assert.assertTrue(((SetTTLPlan) plans.get(0)).isDataBase());
  }

  @Test
  public void testDatabaseTTLAndNarrowerAuthorizedPathsRemainSeparate() throws Exception {
    final SetTTLPlan originalPlan = new SetTTLPlan(100, "root", "db");
    originalPlan.setDataBase(true);
    final List<ConfigPhysicalPlan> plans =
        PipeConfigTreePrivilegeParseVisitor.createTTLPlans(
            Arrays.asList(new PartialPath("root.db.**"), new PartialPath("root.db.device1.**")),
            originalPlan,
            new PartialPath("root.db"),
            new PartialPath("root.db.**"));

    Assert.assertEquals(2, plans.size());
    Assert.assertEquals("root.db", pathOf((SetTTLPlan) plans.get(0)));
    Assert.assertTrue(((SetTTLPlan) plans.get(0)).isDataBase());
    Assert.assertEquals("root.db.device1.**", pathOf((SetTTLPlan) plans.get(1)));
    Assert.assertFalse(((SetTTLPlan) plans.get(1)).isDataBase());
  }

  private static String pathOf(final SetTTLPlan plan) {
    return String.join(".", plan.getPathPattern());
  }
}
