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

package org.apache.iotdb.db.metadata.schemaRegion;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public abstract class AbstractSchemaRegionTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private SchemaRegionTestParams rawConfig;

  protected final SchemaRegionTestParams testParams;

  @Parameterized.Parameters(name = "{0}")
  public static List<SchemaRegionTestParams> getTestModes() {
    return Arrays.asList(
        new SchemaRegionTestParams("MemoryMode", "Memory", -1),
        new SchemaRegionTestParams("PBTree-FullMemory", "PBTree", 10000),
        new SchemaRegionTestParams("PBTree-PartialMemory", "PBTree", 3),
        new SchemaRegionTestParams("PBTree-NonMemory", "PBTree", 0));
  }

  public AbstractSchemaRegionTest(final SchemaRegionTestParams testParams) {
    this.testParams = testParams;
  }

  @Before
  public void setUp() throws Exception {
    rawConfig =
        new SchemaRegionTestParams(
            "Raw-Config",
            COMMON_CONFIG.getSchemaEngineMode(),
            config.getCachedMNodeSizeInPBTreeMode());
    COMMON_CONFIG.setSchemaEngineMode(testParams.schemaEngineMode);
    config.setCachedMNodeSizeInPBTreeMode(testParams.cachedMNodeSize);
    SchemaEngine.getInstance().init();
  }

  @After
  public void tearDown() throws Exception {
    SchemaEngine.getInstance().clear();
    cleanEnv();
    COMMON_CONFIG.setSchemaEngineMode(rawConfig.schemaEngineMode);
    config.setCachedMNodeSizeInPBTreeMode(rawConfig.cachedMNodeSize);
  }

  protected void cleanEnv() throws IOException {
    FileUtils.deleteDirectory(new File(IoTDBDescriptor.getInstance().getConfig().getSchemaDir()));
  }

  protected void simulateRestart() {
    SchemaEngine.getInstance().clear();
    SchemaEngine.getInstance().init();
  }

  protected ISchemaRegion getSchemaRegion(final String database, final int schemaRegionId)
      throws Exception {
    SchemaRegionId regionId = new SchemaRegionId(schemaRegionId);
    if (SchemaEngine.getInstance().getSchemaRegion(regionId) == null) {
      SchemaEngine.getInstance().createSchemaRegion(new PartialPath(database), regionId);
    }
    return SchemaEngine.getInstance().getSchemaRegion(regionId);
  }

  public static class SchemaRegionTestParams {

    private final String testModeName;

    private final String schemaEngineMode;

    private final int cachedMNodeSize;

    private SchemaRegionTestParams(
        final String testModeName, final String schemaEngineMode, final int cachedMNodeSize) {
      this.testModeName = testModeName;
      this.schemaEngineMode = schemaEngineMode;
      this.cachedMNodeSize = cachedMNodeSize;
    }

    public String getTestModeName() {
      return testModeName;
    }

    public String getSchemaEngineMode() {
      return schemaEngineMode;
    }

    public int getCachedMNodeSize() {
      return cachedMNodeSize;
    }

    @Override
    public String toString() {
      return testModeName;
    }
  }
}
