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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.NodeRef;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.commons.udf.service.UDFClassLoader;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.TransformOperator;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class TransformOperatorResourceTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testOperatorsInSameDriverReleaseClassLoaderIndependently() throws Exception {
    testClassLoaderLifetime(false);
  }

  @Test
  public void testClassLoaderReplacementBetweenOperatorsKeepsFirstLoaderAlive() throws Exception {
    testClassLoaderLifetime(true);
  }

  private void testClassLoaderLifetime(boolean replaceBetweenOperators) throws Exception {
    File libRoot = temporaryFolder.newFolder();
    try (JarOutputStream jar =
        new JarOutputStream(new FileOutputStream(new File(libRoot, "test.jar")))) {
      jar.putNextEntry(new JarEntry("udf-resource.txt"));
      jar.write(1);
      jar.closeEntry();
    }
    Constructor<UDFClassLoaderManager> constructor =
        UDFClassLoaderManager.class.getDeclaredConstructor(String.class);
    constructor.setAccessible(true);
    UDFClassLoaderManager manager = constructor.newInstance(libRoot.getAbsolutePath());
    manager.start();
    UDFClassLoader firstLoader = manager.updateAndGetActiveClassLoader();
    UDFClassLoader secondLoader = firstLoader;
    UDFClassLoader activeLoader = null;
    Field instance = UDFClassLoaderManager.class.getDeclaredField("INSTANCE");
    instance.setAccessible(true);
    UDFClassLoaderManager previousManager = UDFClassLoaderManager.getInstance();
    TransformOperator first = null;
    TransformOperator second = null;
    try {
      instance.set(null, manager);
      DriverContext driverContext = new DriverContext();
      driverContext.setDriverTaskID(
          new DriverTaskId(
              new FragmentInstanceId(new PlanFragmentId(new QueryId("udf_resources"), 0), "0"), 0));
      first = newTransform(driverContext, 1);
      if (replaceBetweenOperators) {
        secondLoader = manager.updateAndGetActiveClassLoader();
      }
      second = newTransform(driverContext, 2);
      // CREATE FUNCTION retires the active loader while both operators are still alive.
      activeLoader = manager.updateAndGetActiveClassLoader();
      assertNotNull(firstLoader.findResource("udf-resource.txt"));
      assertNotNull(secondLoader.findResource("udf-resource.txt"));

      first.close();
      first = null;
      if (replaceBetweenOperators) {
        assertNull(firstLoader.findResource("udf-resource.txt"));
      }
      assertNotNull(secondLoader.findResource("udf-resource.txt"));

      second.close();
      second = null;
      assertNull(firstLoader.findResource("udf-resource.txt"));
      assertNull(secondLoader.findResource("udf-resource.txt"));
    } finally {
      try {
        if (first != null) {
          first.close();
        }
        if (second != null) {
          second.close();
        }
      } finally {
        instance.set(null, previousManager);
        firstLoader.close();
        secondLoader.close();
        if (activeLoader != null) {
          activeLoader.close();
        }
      }
    }
  }

  private TransformOperator newTransform(DriverContext driverContext, int operatorId)
      throws Exception {
    TimeSeriesOperand input =
        new TimeSeriesOperand(new PartialPath("root.review.d.s1"), TSDataType.INT64);
    return new TransformOperator(
        driverContext.addOperatorContext(
            operatorId, new PlanNodeId("transform" + operatorId), "TransformOperator"),
        mock(Operator.class),
        ImmutableList.of(TSDataType.INT64),
        ImmutableMap.of(input.getExpressionString(), ImmutableList.of(new InputLocation(0, 0))),
        new Expression[] {input},
        true,
        ZoneId.systemDefault(),
        ImmutableMap.of(NodeRef.of(input), TSDataType.INT64),
        true);
  }
}
