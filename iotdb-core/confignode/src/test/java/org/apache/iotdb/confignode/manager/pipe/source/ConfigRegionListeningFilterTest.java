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

package org.apache.iotdb.confignode.manager.pipe.source;

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.table.Audit;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Set;

public class ConfigRegionListeningFilterTest {

  @Test
  public void testAuthUserRevokeUsesRevokeRelationalPlans() throws Exception {
    final Set<ConfigPhysicalPlanType> listenedPlanTypes =
        ConfigRegionListeningFilter.parseListeningPlanTypeSet(
            new PipeParameters(
                new HashMap<String, String>() {
                  {
                    put(PipeSourceConstant.EXTRACTOR_INCLUSION_KEY, "auth.user.revoke");
                  }
                }));

    Assert.assertTrue(listenedPlanTypes.contains(ConfigPhysicalPlanType.RevokeUser));
    Assert.assertTrue(listenedPlanTypes.contains(ConfigPhysicalPlanType.RevokeRoleFromUser));
    Assert.assertTrue(listenedPlanTypes.contains(ConfigPhysicalPlanType.RRevokeUserRole));
    Assert.assertTrue(listenedPlanTypes.contains(ConfigPhysicalPlanType.RRevokeUserAll));
    Assert.assertTrue(listenedPlanTypes.contains(ConfigPhysicalPlanType.RRevokeUserSysPri));
    Assert.assertFalse(listenedPlanTypes.contains(ConfigPhysicalPlanType.RGrantUserAll));
    Assert.assertFalse(listenedPlanTypes.contains(ConfigPhysicalPlanType.RGrantUserSysPri));
  }

  @Test
  public void testInternalDatabasesAreFilteredForCreateAndAlter() {
    Assert.assertFalse(
        ConfigRegionListeningFilter.shouldPlanBeListened(
            new DatabaseSchemaPlan(
                ConfigPhysicalPlanType.CreateDatabase,
                new TDatabaseSchema(SchemaConstant.SYSTEM_DATABASE))));
    Assert.assertFalse(
        ConfigRegionListeningFilter.shouldPlanBeListened(
            new DatabaseSchemaPlan(
                ConfigPhysicalPlanType.CreateDatabase,
                new TDatabaseSchema(SchemaConstant.AUDIT_DATABASE))));
    Assert.assertFalse(
        ConfigRegionListeningFilter.shouldPlanBeListened(
            new DatabaseSchemaPlan(
                ConfigPhysicalPlanType.CreateDatabase,
                new TDatabaseSchema(Audit.TABLE_MODEL_AUDIT_DATABASE))));

    Assert.assertFalse(
        ConfigRegionListeningFilter.shouldPlanBeListened(
            new DatabaseSchemaPlan(
                ConfigPhysicalPlanType.AlterDatabase,
                new TDatabaseSchema(SchemaConstant.SYSTEM_DATABASE))));
    Assert.assertFalse(
        ConfigRegionListeningFilter.shouldPlanBeListened(
            new DatabaseSchemaPlan(
                ConfigPhysicalPlanType.AlterDatabase,
                new TDatabaseSchema(SchemaConstant.AUDIT_DATABASE))));
    Assert.assertFalse(
        ConfigRegionListeningFilter.shouldPlanBeListened(
            new DatabaseSchemaPlan(
                ConfigPhysicalPlanType.AlterDatabase,
                new TDatabaseSchema(Audit.TABLE_MODEL_AUDIT_DATABASE))));

    Assert.assertTrue(
        ConfigRegionListeningFilter.shouldPlanBeListened(
            new DatabaseSchemaPlan(
                ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.db"))));
    Assert.assertTrue(
        ConfigRegionListeningFilter.shouldPlanBeListened(
            new DatabaseSchemaPlan(
                ConfigPhysicalPlanType.AlterDatabase, new TDatabaseSchema("root.db"))));
  }
}
