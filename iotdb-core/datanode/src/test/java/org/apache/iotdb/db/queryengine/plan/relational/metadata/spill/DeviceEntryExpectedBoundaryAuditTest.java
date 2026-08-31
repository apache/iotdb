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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertThrows;

public class DeviceEntryExpectedBoundaryAuditTest {

  private Path temporaryDirectory;

  @Before
  public void setUp() throws Exception {
    temporaryDirectory = Files.createTempDirectory("device-entry-boundary-audit");
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(temporaryDirectory.toFile());
  }

  @Test
  public void shouldRejectNegativeSegmentCount() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DeviceEntryDataSetHandle(
                "q-invalid",
                new PlanNodeId("scan-0"),
                new TEndPoint("127.0.0.1", 10740),
                -1,
                0,
                false));
  }

  @Test
  public void shouldRejectNegativeEntryCount() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DeviceEntryDataSetHandle(
                "q-invalid",
                new PlanNodeId("scan-0"),
                new TEndPoint("127.0.0.1", 10740),
                0,
                -1,
                false));
  }
}
