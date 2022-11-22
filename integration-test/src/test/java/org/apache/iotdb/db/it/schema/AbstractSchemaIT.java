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
import org.apache.iotdb.it.framework.IoTDBTestRunnerWithParameters;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.ParametersRunnerFactory;
import org.junit.runners.parameterized.TestWithParameters;

import java.util.Arrays;

@RunWith(Parameterized.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public abstract class AbstractSchemaIT {

  protected String testSchemaEngineMode;
  private String defaultSchemaEngineMode;

  @Parameterized.Parameters(name = "SchemaEngineMode={0}")
  public static Iterable<String> data() {
    return Arrays.asList("Memory", "Schema_File");
  }

  public AbstractSchemaIT(String schemaEngineMode) {
    this.testSchemaEngineMode = schemaEngineMode;
  }

  @Before
  public void setUp() throws Exception {
    defaultSchemaEngineMode = ConfigFactory.getConfig().getSchemaEngineMode();
    ConfigFactory.getConfig().setSchemaEngineMode(testSchemaEngineMode);
  }

  @After
  public void tearDown() throws Exception {
    ConfigFactory.getConfig().setSchemaEngineMode(defaultSchemaEngineMode);
  }

  public static class RunnerFactory implements ParametersRunnerFactory {
    @Override
    public org.junit.runner.Runner createRunnerForTestWithParameters(TestWithParameters test)
        throws InitializationError {
      return new IoTDBTestRunnerWithParameters(test);
    }
  }
}
