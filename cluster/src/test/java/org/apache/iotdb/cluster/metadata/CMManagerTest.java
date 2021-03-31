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

package org.apache.iotdb.cluster.metadata;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.query.BaseQueryTest;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CMManagerTest extends BaseQueryTest {
  private CMManager cmManager;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    cmManager = CMManager.getInstance();
  }

  @Test
  public void testGetAllTimeseriesPath() throws MetadataException {
    PartialPath partialPath = new PartialPath(TestUtils.getTestSg(0));
    List<PartialPath> partialPaths = cmManager.getAllTimeseriesPath(partialPath);
    assertEquals("root.test0.s3", partialPaths.get(0).getFullPath());
  }

  @Test
  public void testGetMatchedPathsDiablePathCache() throws MetadataException {
    PartialPath partialPath = new PartialPath(TestUtils.getTestSg(0));
    List<PartialPath> partialPaths = cmManager.getMatchedPaths(partialPath);
    assertEquals("root.test0.s3", partialPaths.get(0).getFullPath());
  }
}
