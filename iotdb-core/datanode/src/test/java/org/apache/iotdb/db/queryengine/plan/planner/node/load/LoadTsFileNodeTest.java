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

package org.apache.iotdb.db.queryengine.plan.planner.node.load;

import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.NotImplementedException;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;

public class LoadTsFileNodeTest {

  @Test
  public void testLoadSingleTsFileNode() {
    TsFileResource resource = new TsFileResource(new File("1"));
    LoadSingleTsFileNode node = new LoadSingleTsFileNode(new PlanNodeId(""), resource, true, 0L);
    Assert.assertTrue(node.isDeleteAfterLoad());
    Assert.assertEquals(resource, node.getTsFileResource());
    Assert.assertNull(node.getLocalRegionReplicaSet());
    Assert.assertNull(node.getRegionReplicaSet());
    Assert.assertEquals(Collections.emptyList(), node.getChildren());
    Assert.assertEquals(Collections.emptyList(), node.getOutputColumnNames());
    try {
      node.clone();
      Assert.fail();
    } catch (NotImplementedException ignored) {
    }
    try {
      node.splitByPartition(new Analysis());
      Assert.fail();
    } catch (NotImplementedException ignored) {
    }
    Assert.assertEquals(0, node.allowedChildCount());
    Assert.assertEquals("LoadSingleTsFileNode{tsFile=1, needDecodeTsFile=false}", node.toString());
    node.clean();
  }

  @Test
  public void testLoadTsFilePieceNode() {
    LoadTsFilePieceNode node = new LoadTsFilePieceNode(new PlanNodeId(""), new File("1"));
    Assert.assertEquals(0, node.getDataSize());
    Assert.assertEquals(new ArrayList<>(), node.getAllTsFileData());
    Assert.assertEquals(node.getTsFile(), new File("1"));
    Assert.assertNull(node.getRegionReplicaSet());
    Assert.assertEquals(Collections.emptyList(), node.getChildren());
    try {
      node.clone();
      Assert.fail();
    } catch (NotImplementedException ignored) {
    }
    try {
      node.splitByPartition(new Analysis());
      Assert.fail();
    } catch (NotImplementedException ignored) {
    }
    Assert.assertEquals(0, node.allowedChildCount());
    Assert.assertEquals("LoadTsFilePieceNode{tsFile=1, dataSize=0}", node.toString());
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    node.serialize(buffer);
    LoadTsFilePieceNode node1 = (LoadTsFilePieceNode) LoadTsFilePieceNode.deserialize(buffer);
    Assert.assertEquals(node.getTsFile(), node1.getTsFile());
  }

  @Test
  public void testCleanContinuesAfterOneFileCannotBeDeleted() throws Exception {
    final File tempDir = Files.createTempDirectory("load-node-clean").toFile();
    try {
      // A non-empty directory at the TsFile path makes that deletion fail deterministically. The
      // companion cleanup must still continue instead of sharing the same try-catch block.
      final File tsFile = new File(tempDir, "1-0-0-0.tsfile");
      Assert.assertTrue(tsFile.mkdirs());
      Assert.assertTrue(new File(tsFile, "non-empty").createNewFile());
      final File resourceFile = new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
      final File modsFile = new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
      Assert.assertTrue(resourceFile.createNewFile());
      Assert.assertTrue(modsFile.createNewFile());

      final LoadSingleTsFileNode node =
          new LoadSingleTsFileNode(new PlanNodeId(""), new TsFileResource(tsFile), true, 0L);
      node.clean();

      Assert.assertTrue(tsFile.exists());
      Assert.assertFalse(resourceFile.exists());
      Assert.assertFalse(modsFile.exists());
    } finally {
      deleteRecursively(tempDir);
    }
  }

  private static void deleteRecursively(final File file) {
    if (file == null || !file.exists()) {
      return;
    }
    final File[] children = file.listFiles();
    if (children != null) {
      for (final File child : children) {
        deleteRecursively(child);
      }
    }
    Assert.assertTrue(file.delete());
  }
}
