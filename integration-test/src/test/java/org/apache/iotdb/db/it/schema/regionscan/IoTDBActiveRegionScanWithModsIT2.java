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

package org.apache.iotdb.db.it.schema.regionscan;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBActiveRegionScanWithModsIT2 extends IoTDBActiveRegionScanWithModsIT {
  public IoTDBActiveRegionScanWithModsIT2(final SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setMaxTsBlockLineNumber(1);
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    tearDownEnvironment();
    EnvFactory.getEnv().cleanClusterEnvironment();
  }
}
