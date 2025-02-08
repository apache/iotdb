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

package org.apache.iotdb.util;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunnerWithParametersFactory;

import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * This class define multiple modes for schema engine. All IT class extends AbstractSchemaIT will be
 * run in both Memory and PBTree modes. In PBTree mode, there are three kinds of test environment:
 * full memory, partial memory and non memory.
 *
 * <p>Notice that, all IT class extends AbstractSchemaIT need to call {@link
 * AbstractSchemaIT#setUpEnvironment} before test env initialization at {@code @BeforeParam}, or
 * call {@link AbstractSchemaIT#setUpEnvironmentBeforeMethod()} at {@code @Before}, and call {@link
 * AbstractSchemaIT#tearDownEnvironment} after test env cleaning.
 */
@RunWith(Parameterized.class)
@NotThreadSafe
@Parameterized.UseParametersRunnerFactory(IoTDBTestRunnerWithParametersFactory.class)
public abstract class AbstractSchemaIT {

  protected SchemaTestMode schemaTestMode;

  protected static final List<SchemaTestMode> schemaTestModes =
      Arrays.asList(SchemaTestMode.Memory, SchemaTestMode.PBTree);

  private static int mode = 0;

  @Parameterized.Parameters(name = "SchemaEngineMode={0}")
  public static Iterable<SchemaTestMode> data() {
    return Arrays.asList(SchemaTestMode.Memory, SchemaTestMode.PBTree);
  }

  public AbstractSchemaIT(SchemaTestMode schemaTestMode) {
    this.schemaTestMode = schemaTestMode;
  }

  @BeforeClass
  public static void beforeClass() {
    mode = 0;
  }

  protected static SchemaTestMode setUpEnvironment() throws Exception {
    return setUpEnvironmentInternal(schemaTestModes.get(mode++));
  }

  protected void setUpEnvironmentBeforeMethod() {
    setUpEnvironmentInternal(schemaTestMode);
  }

  private static SchemaTestMode setUpEnvironmentInternal(final SchemaTestMode schemaTestMode) {
    switch (schemaTestMode) {
      case Memory:
        EnvFactory.getEnv().getConfig().getCommonConfig().setSchemaEngineMode("Memory");
        break;
      case PBTree:
        EnvFactory.getEnv().getConfig().getCommonConfig().setSchemaEngineMode("PBTree");
        allocateMemoryForSchemaRegion(4000);
        break;
    }
    return schemaTestMode;
  }

  protected static void tearDownEnvironment() throws Exception {}

  protected static void clearSchema() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("DELETE DATABASE root.**");
      } catch (Exception e) {
        // If database is null, it will throw exception. Do nothing.
      }
      // delete all template
      try (ResultSet resultSet = statement.executeQuery("SHOW DEVICE TEMPLATES")) {
        while (resultSet.next()) {
          statement.execute(
              "DROP DEVICE TEMPLATE " + resultSet.getString(ColumnHeaderConstant.TEMPLATE_NAME));
        }
      }
      // drop all users
      try (ResultSet resultSet = statement.executeQuery("LIST USER")) {
        while (resultSet.next()) {
          if (!resultSet.getString(ColumnHeaderConstant.USER).equals("root")) {
            statement.execute("DROP USER " + resultSet.getString(ColumnHeaderConstant.USER));
          }
        }
      }
    }
  }

  /**
   * Set memory allocated to the SchemaRegion. There is no guarantee that the memory allocated to
   * the schemaRegion will be exactly equal to the set memory, but it will be greater than the set
   * memory.
   *
   * @param allocateMemoryForSchemaRegion bytes
   */
  protected static void allocateMemoryForSchemaRegion(int allocateMemoryForSchemaRegion) {
    int schemaAllMemory = 25742540;
    int sumProportion = schemaAllMemory / allocateMemoryForSchemaRegion;
    int[] proportion = new int[] {1, (sumProportion - 1) * 3 / 4, (sumProportion - 1) / 4};
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaMemoryAllocate(StringUtils.join(proportion, ':'));
  }

  public enum SchemaTestMode {
    Memory,
    PBTree
  }
}
