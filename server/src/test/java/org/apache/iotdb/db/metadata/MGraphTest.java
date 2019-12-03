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
package org.apache.iotdb.db.metadata;

import static org.junit.Assert.*;

import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StorageGroupException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MGraphTest {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testCombineMetadataInStrings() {
    MGraph root = new MGraph("root");
    MGraph root1 = new MGraph("root");
    MGraph root2 = new MGraph("root");
    MGraph root3 = new MGraph("root");
    try {
      root.setStorageGroup("root.a.d0");
      root.addPathToMTree("root.a.d0.s0", "INT32", "RLE");
      root.addPathToMTree("root.a.d0.s1", "INT32", "RLE");

      root.setStorageGroup("root.a.d1");
      root.addPathToMTree("root.a.d1.s0", "INT32", "RLE");
      root.addPathToMTree("root.a.d1.s1", "INT32", "RLE");

      root.setStorageGroup("root.a.b.d0");
      root.addPathToMTree("root.a.b.d0.s0", "INT32", "RLE");

      root1.setStorageGroup("root.a.d0");
      root1.addPathToMTree("root.a.d0.s0", "INT32", "RLE");
      root1.addPathToMTree("root.a.d0.s1", "INT32", "RLE");

      root2.setStorageGroup("root.a.d1");
      root2.addPathToMTree("root.a.d1.s0", "INT32", "RLE");
      root2.addPathToMTree("root.a.d1.s1", "INT32", "RLE");

      root3.setStorageGroup("root.a.b.d0");
      root3.addPathToMTree("root.a.b.d0.s0", "INT32", "RLE");

      String[] metadatas = new String[3];
      metadatas[0] = root1.toString();
      metadatas[1] = root2.toString();
      metadatas[2] = root3.toString();
      assertEquals(MGraph.combineMetadataInStrings(metadatas), root.toString());
    } catch (MetadataErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}