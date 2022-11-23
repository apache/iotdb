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
package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunnerWithParametersFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

/**
 * This class define multiple modes for schema engine. All IT class extends AbstractSchemaIT will be
 * run in both Memory and Schema_File modes. In Schema_File mode, there are three kinds of test
 * environment: full memory, partial memory and non memory.
 *
 * <p>Notice that, all IT class extends AbstractSchemaIT need to call {@link AbstractSchemaIT#setUp}
 * before test env initialization and call {@link AbstractSchemaIT#tearDown} after test env
 * cleaning. initialize
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(IoTDBTestRunnerWithParametersFactory.class)
public abstract class AbstractSchemaIT {

  protected SchemaTestMode schemaTestMode;
  private String defaultSchemaEngineMode;
  private int defaultCachedMNodeSizeInSchemaFileMode;

  @Parameterized.Parameters(name = "SchemaEngineMode={0}")
  public static Iterable<SchemaTestMode> data() {
    return Arrays.asList(
        SchemaTestMode.Memory,
        SchemaTestMode.SchemaFileFullMemory,
        SchemaTestMode.SchemaFilePartialMemory,
        SchemaTestMode.SchemaFileNonMemory);
  }

  public AbstractSchemaIT(SchemaTestMode schemaTestMode) {
    this.schemaTestMode = schemaTestMode;
  }

  @Before
  public void setUp() throws Exception {
    defaultSchemaEngineMode = ConfigFactory.getConfig().getSchemaEngineMode();
    defaultCachedMNodeSizeInSchemaFileMode =
        ConfigFactory.getConfig().getCachedMNodeSizeInSchemaFileMode();
    switch (schemaTestMode) {
      case Memory:
        ConfigFactory.getConfig().setSchemaEngineMode("Memory");
        break;
      case SchemaFileFullMemory:
        ConfigFactory.getConfig().setSchemaEngineMode("Schema_File");
        break;
      case SchemaFilePartialMemory:
        ConfigFactory.getConfig().setSchemaEngineMode("Schema_File");
        ConfigFactory.getConfig().setCachedMNodeSizeInSchemaFileMode(3);
        break;
      case SchemaFileNonMemory:
        ConfigFactory.getConfig().setSchemaEngineMode("Schema_File");
        ConfigFactory.getConfig().setCachedMNodeSizeInSchemaFileMode(0);
        break;
    }
  }

  @After
  public void tearDown() throws Exception {
    ConfigFactory.getConfig().setSchemaEngineMode(defaultSchemaEngineMode);
    ConfigFactory.getConfig()
        .setCachedMNodeSizeInSchemaFileMode(defaultCachedMNodeSizeInSchemaFileMode);
  }

  enum SchemaTestMode {
    Memory,
    SchemaFileFullMemory,
    SchemaFilePartialMemory,
    SchemaFileNonMemory
  }
}
