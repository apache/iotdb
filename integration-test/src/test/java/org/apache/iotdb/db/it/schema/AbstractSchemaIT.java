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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunnerWithParametersFactory;

import net.jcip.annotations.NotThreadSafe;
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
 * cleaning.
 */
@RunWith(Parameterized.class)
@NotThreadSafe
@Parameterized.UseParametersRunnerFactory(IoTDBTestRunnerWithParametersFactory.class)
public abstract class AbstractSchemaIT {

  protected SchemaTestMode schemaTestMode;

  @Parameterized.Parameters(name = "SchemaEngineMode={0}")
  public static Iterable<SchemaTestMode> data() {
    return Arrays.asList(SchemaTestMode.Memory, SchemaTestMode.SchemaFile);
  }

  public AbstractSchemaIT(SchemaTestMode schemaTestMode) {
    this.schemaTestMode = schemaTestMode;
  }

  public void setUp() throws Exception {
    switch (schemaTestMode) {
      case Memory:
        EnvFactory.getEnv().getConfig().getCommonConfig().setSchemaEngineMode("Memory");
        break;
      case SchemaFile:
        EnvFactory.getEnv().getConfig().getCommonConfig().setSchemaEngineMode("Schema_File");
        EnvFactory.getEnv()
            .getConfig()
            .getCommonConfig()
            .setSchemaMemoryAllocate("3:10000:5000:5000");
        break;
    }
  }

  public void tearDown() throws Exception {}

  enum SchemaTestMode {
    Memory,
    SchemaFile
  }
}
