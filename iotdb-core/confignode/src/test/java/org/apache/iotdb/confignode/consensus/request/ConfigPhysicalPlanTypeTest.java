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

package org.apache.iotdb.confignode.consensus.request;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfigPhysicalPlanTypeTest {

  @Test
  public void checkUniqueness() {
    Map<Short, ConfigPhysicalPlanType> map = new HashMap<>();
    for (ConfigPhysicalPlanType value : ConfigPhysicalPlanType.values()) {
      if (map.containsKey(value.getPlanType())) {
        fail(
            String.format(
                "%s and %s have the same type number %s",
                map.get(value.getPlanType()), value, value.getPlanType()));
      } else {
        map.put(value.getPlanType(), value);
      }
    }
  }

  @Test
  public void writableViewPlanTypesShouldBeNegative() {
    Arrays.asList(
            ConfigPhysicalPlanType.RollbackCreateWritableView,
            ConfigPhysicalPlanType.CommitCreateWritableView,
            ConfigPhysicalPlanType.AddWritableViewColumn,
            ConfigPhysicalPlanType.CommitDeleteWritableViewColumn,
            ConfigPhysicalPlanType.CommitDeleteWritableView,
            ConfigPhysicalPlanType.RenameWritableView,
            ConfigPhysicalPlanType.SetWritableViewProperties,
            ConfigPhysicalPlanType.PreDeleteWritableViewColumn,
            ConfigPhysicalPlanType.PreDeleteWritableView,
            ConfigPhysicalPlanType.RenameWritableViewColumn,
            ConfigPhysicalPlanType.PreAlterWritableViewColumnDataType,
            ConfigPhysicalPlanType.AlterWritableViewColumnDataType,
            ConfigPhysicalPlanType.SetWritableViewColumnComment,
            ConfigPhysicalPlanType.SetWritableViewComment)
        .forEach(
            planType ->
                assertTrue(
                    planType + " should use a negative planType id", planType.getPlanType() < 0));
  }

  @Test
  public void writableViewPlanTypesShouldBeConvertibleFromNegativeIds() {
    Arrays.asList(
            ConfigPhysicalPlanType.RollbackCreateWritableView,
            ConfigPhysicalPlanType.CommitCreateWritableView,
            ConfigPhysicalPlanType.AddWritableViewColumn,
            ConfigPhysicalPlanType.CommitDeleteWritableViewColumn,
            ConfigPhysicalPlanType.CommitDeleteWritableView,
            ConfigPhysicalPlanType.RenameWritableView,
            ConfigPhysicalPlanType.SetWritableViewProperties,
            ConfigPhysicalPlanType.PreDeleteWritableViewColumn,
            ConfigPhysicalPlanType.PreDeleteWritableView,
            ConfigPhysicalPlanType.RenameWritableViewColumn,
            ConfigPhysicalPlanType.PreAlterWritableViewColumnDataType,
            ConfigPhysicalPlanType.AlterWritableViewColumnDataType,
            ConfigPhysicalPlanType.SetWritableViewColumnComment,
            ConfigPhysicalPlanType.SetWritableViewComment)
        .forEach(
            planType ->
                assertSame(
                    planType,
                    ConfigPhysicalPlanType.convertToConfigPhysicalPlanType(
                        planType.getPlanType())));
  }
}
