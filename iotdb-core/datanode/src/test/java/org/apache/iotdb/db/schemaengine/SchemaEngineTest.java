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
package org.apache.iotdb.db.schemaengine;

import org.apache.iotdb.commons.consensus.SchemaRegionId;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaEngineTest {

  @Test
  public void uniqueSchemaRegionIdsPassRecoveryValidation() {
    final Map<String, List<SchemaRegionId>> localSchemaRegionInfo = new HashMap<>();
    localSchemaRegionInfo.put(
        "root.db1", Arrays.asList(new SchemaRegionId(0), new SchemaRegionId(1)));
    localSchemaRegionInfo.put("root.db2", Collections.singletonList(new SchemaRegionId(2)));

    SchemaEngine.validateNoDuplicatedSchemaRegionId(localSchemaRegionInfo);
  }

  @Test
  public void duplicatedSchemaRegionIdIsRejectedBeforeRecovery() {
    final Map<String, List<SchemaRegionId>> localSchemaRegionInfo = new HashMap<>();
    localSchemaRegionInfo.put("root.db1", Collections.singletonList(new SchemaRegionId(0)));
    localSchemaRegionInfo.put("root.db2", Collections.singletonList(new SchemaRegionId(0)));

    final IllegalStateException exception =
        Assert.assertThrows(
            IllegalStateException.class,
            () -> SchemaEngine.validateNoDuplicatedSchemaRegionId(localSchemaRegionInfo));
    Assert.assertTrue(exception.getMessage().contains("root.db1"));
    Assert.assertTrue(exception.getMessage().contains("root.db2"));
  }
}
