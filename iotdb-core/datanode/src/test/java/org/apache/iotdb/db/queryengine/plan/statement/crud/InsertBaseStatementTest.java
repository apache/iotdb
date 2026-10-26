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

package org.apache.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class InsertBaseStatementTest {

  @Test
  public void testPathsStringForLog() throws IllegalPathException {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final int oldSize = config.getPathLogMaxSize();
    final InsertRowsOfOneDeviceStatement statement = new InsertRowsOfOneDeviceStatement();
    final List<MeasurementPath> paths =
        Arrays.asList(
            new MeasurementPath("root.db.device.s1"),
            new MeasurementPath("root.db.device.s2"),
            new MeasurementPath("root.db.device.s3"),
            new MeasurementPath("root.db.device.s4"));
    try {
      config.setPathLogMaxSize(2);
      Assert.assertEquals(
          "[root.db.device.s1, root.db.device.s2, ...]", statement.getPathsStringForLog(paths));
      Assert.assertEquals(
          "[root.db.device.s1, root.db.device.s2]",
          statement.getPathsStringForLog(paths.subList(0, 2)));
      Assert.assertEquals("[]", statement.getPathsStringForLog(Collections.emptyList()));
    } finally {
      config.setPathLogMaxSize(oldSize);
    }
  }

  @Test
  public void testPathsStringForLogIsLazy() throws IllegalPathException {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final int oldSize = config.getPathLogMaxSize();
    final AtomicInteger createdPathCount = new AtomicInteger();
    final PartialPath devicePath =
        new PartialPath("root.db.device") {
          @Override
          public MeasurementPath concatAsMeasurementPath(String measurement) {
            createdPathCount.incrementAndGet();
            return super.concatAsMeasurementPath(measurement);
          }
        };
    final InsertRowsOfOneDeviceStatement statement = new InsertRowsOfOneDeviceStatement();
    statement.setDevicePath(devicePath);
    statement.setMeasurements(new String[] {"s1", "s2", "s3", "s4"});

    try {
      config.setPathLogMaxSize(2);
      Assert.assertEquals(
          "[root.db.device.s1, root.db.device.s2, ...]", statement.getPathsStringForLog());
      Assert.assertEquals(3, createdPathCount.get());
    } finally {
      config.setPathLogMaxSize(oldSize);
    }
  }
}
