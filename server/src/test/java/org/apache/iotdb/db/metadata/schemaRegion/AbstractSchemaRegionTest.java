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

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;

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

  private SchemaRegionTestParams rawConfig;

  protected final SchemaRegionTestParams testParams;

  @Parameterized.Parameters(name = "{0}")
  public static List<SchemaRegionTestParams> getTestModes() {
    return Arrays.asList(
        new SchemaRegionTestParams("MemoryMode", "Memory", -1, true),
        new SchemaRegionTestParams("SchemaFile-FullMemory", "Schema_File", 10000, true),
        new SchemaRegionTestParams("SchemaFile-PartialMemory", "Schema_File", 3, true),
        new SchemaRegionTestParams("SchemaFile-NonMemory", "Schema_File", 0, true));
  }

  public AbstractSchemaRegionTest(SchemaRegionTestParams testParams) {
    this.testParams = testParams;
  }

  @Before
  public void setUp() throws Exception {
    rawConfig =
        new SchemaRegionTestParams(
            "Raw-Config",
            config.getSchemaEngineMode(),
            config.getCachedMNodeSizeInSchemaFileMode(),
            config.isClusterMode());
    config.setSchemaEngineMode(testParams.schemaEngineMode);
    config.setCachedMNodeSizeInSchemaFileMode(testParams.cachedMNodeSize);
    config.setClusterMode(testParams.isClusterMode);
    SchemaEngine.getInstance().init();
  }

  @After
  public void tearDown() throws Exception {
    SchemaEngine.getInstance().clear();
    cleanEnv();
    config.setSchemaEngineMode(rawConfig.schemaEngineMode);
    config.setCachedMNodeSizeInSchemaFileMode(rawConfig.cachedMNodeSize);
    config.setClusterMode(rawConfig.isClusterMode);
  }

  protected void cleanEnv() throws IOException {
    FileUtils.deleteDirectory(new File(IoTDBDescriptor.getInstance().getConfig().getSchemaDir()));
  }

  protected void simulateRestart() {
    SchemaEngine.getInstance().clear();
    SchemaEngine.getInstance().init();
  }

  protected ISchemaRegion getSchemaRegion(String database, int schemaRegionId) throws Exception {
    SchemaRegionId regionId = new SchemaRegionId(schemaRegionId);
    if (SchemaEngine.getInstance().getSchemaRegion(regionId) == null) {
      SchemaEngine.getInstance().createSchemaRegion(new PartialPath(database), regionId);
    }
    return SchemaEngine.getInstance().getSchemaRegion(regionId);
  }

  protected static class SchemaRegionTestParams {

    private final String testModeName;

    private final String schemaEngineMode;

    private final int cachedMNodeSize;

    private final boolean isClusterMode;

    private SchemaRegionTestParams(
        String testModeName, String schemaEngineMode, int cachedMNodeSize, boolean isClusterMode) {
      this.testModeName = testModeName;
      this.schemaEngineMode = schemaEngineMode;
      this.cachedMNodeSize = cachedMNodeSize;
      this.isClusterMode = isClusterMode;
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

    public boolean isClusterMode() {
      return isClusterMode;
    }

    @Override
    public String toString() {
      return testModeName;
    }
  }
}
