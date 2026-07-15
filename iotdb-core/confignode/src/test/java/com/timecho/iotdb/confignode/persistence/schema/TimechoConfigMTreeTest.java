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

package com.timecho.iotdb.confignode.persistence.schema;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.persistence.schema.ConfigMTree;

import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TimechoConfigMTreeTest {

  @Test
  public void testPipeRenameTimeSeriesKeepsFinalAliasOnly() throws Exception {
    final ConfigMTree tree = new ConfigMTree(false);
    tree.setStorageGroup(new PartialPath("root.sg"));

    final PartialPath physicalPath = new PartialPath("root.sg.d1.s_physical");
    final PartialPath firstAliasPath = new PartialPath("root.sg.d1.s_alias_1");
    final PartialPath secondAliasPath = new PartialPath("root.sg.d1.s_alias_2");
    final PartialPath thirdAliasPath = new PartialPath("root.sg.d1.s_alias_3");

    tree.recordPipeRenameTimeSeries(physicalPath, firstAliasPath);
    assertPipeRenameTimeSeriesPlan(
        tree.getPipeRenameTimeSeriesList(), physicalPath, firstAliasPath);
    assertPipeRenameTimeSeriesPathList(tree, physicalPath, firstAliasPath);
    Assert.assertEquals(firstAliasPath, tree.getPipeRenamedAliasPath(physicalPath));

    tree.recordPipeRenameTimeSeries(firstAliasPath, secondAliasPath);
    assertPipeRenameTimeSeriesPlan(
        tree.getPipeRenameTimeSeriesList(), physicalPath, secondAliasPath);
    assertPipeRenameTimeSeriesPathList(tree, physicalPath, secondAliasPath);
    Assert.assertEquals(secondAliasPath, tree.getPipeRenamedAliasPath(physicalPath));

    tree.recordPipeRenameTimeSeries(secondAliasPath, thirdAliasPath);
    assertPipeRenameTimeSeriesPlan(
        tree.getPipeRenameTimeSeriesList(), physicalPath, thirdAliasPath);
    assertPipeRenameTimeSeriesPathList(tree, physicalPath, thirdAliasPath);
  }

  @Test
  public void testPipeRenameTimeSeriesKeepsFinalPhysicalPath() throws Exception {
    final ConfigMTree tree = new ConfigMTree(false);
    tree.setStorageGroup(new PartialPath("root.sg"));

    final PartialPath physicalPath = new PartialPath("root.sg.d1.s_physical");
    final PartialPath aliasPath = new PartialPath("root.sg.d1.s_alias");

    tree.recordPipeRenameTimeSeries(physicalPath, aliasPath);
    tree.recordPipeRenameTimeSeries(aliasPath, physicalPath);

    assertPipeRenameTimeSeriesPlan(tree.getPipeRenameTimeSeriesList(), physicalPath, physicalPath);
    assertPipeRenameTimeSeriesPathList(tree, physicalPath, physicalPath);
  }

  @Test
  public void testPipeRenameTimeSeriesKeepsFinalAliasAfterRepeatedRenames() throws Exception {
    final ConfigMTree tree = new ConfigMTree(false);
    tree.setStorageGroup(new PartialPath("root.sg"));

    final PartialPath physicalPath = new PartialPath("root.sg.d1.s_physical");
    final PartialPath firstAliasPath = new PartialPath("root.sg.d1.s_alias_1");
    final PartialPath secondAliasPath = new PartialPath("root.sg.d1.s_alias_2");
    final PartialPath thirdAliasPath = new PartialPath("root.sg.d1.s_alias_3");
    final PartialPath fourthAliasPath = new PartialPath("root.sg.d1.s_alias_4");

    recordPipeRenameTimeSeriesPathChain(
        tree,
        physicalPath,
        firstAliasPath,
        secondAliasPath,
        thirdAliasPath,
        firstAliasPath,
        secondAliasPath,
        thirdAliasPath,
        fourthAliasPath);

    assertPipeRenameTimeSeriesPlan(
        tree.getPipeRenameTimeSeriesList(), physicalPath, fourthAliasPath);
    assertPipeRenameTimeSeriesPathList(tree, physicalPath, fourthAliasPath);
  }

  @Test
  public void testPipeRenameTimeSeriesRemoveByLatestAliasPath() throws Exception {
    final ConfigMTree tree = new ConfigMTree(false);
    tree.setStorageGroup(new PartialPath("root.sg"));

    final PartialPath physicalPath = new PartialPath("root.sg.d1.s_physical");
    final PartialPath firstAliasPath = new PartialPath("root.sg.d1.s_alias_1");
    final PartialPath secondAliasPath = new PartialPath("root.sg.d1.s_alias_2");

    recordPipeRenameTimeSeriesPathChain(tree, physicalPath, firstAliasPath, secondAliasPath);

    final PathPatternTree oldAliasPatternTree = new PathPatternTree();
    oldAliasPatternTree.appendPathPattern(firstAliasPath);
    oldAliasPatternTree.constructTree();
    tree.removePipeRenameTimeSeries(oldAliasPatternTree);
    assertPipeRenameTimeSeriesPlan(
        tree.getPipeRenameTimeSeriesList(), physicalPath, secondAliasPath);

    final PathPatternTree latestAliasPatternTree = new PathPatternTree();
    latestAliasPatternTree.appendPathPattern(secondAliasPath);
    latestAliasPatternTree.constructTree();
    tree.removePipeRenameTimeSeries(latestAliasPatternTree);
    Assert.assertTrue(tree.getPipeRenameTimeSeriesPaths().isEmpty());
  }

  private void recordPipeRenameTimeSeriesPathChain(
      final ConfigMTree tree, final PartialPath... pathChain) throws Exception {
    for (int i = 1; i < pathChain.length; i++) {
      tree.recordPipeRenameTimeSeries(pathChain[i - 1], pathChain[i]);
    }
  }

  private void assertPipeRenameTimeSeriesPathList(
      final ConfigMTree tree, final PartialPath physicalPath, final PartialPath aliasPath) {
    final List<Pair<String, String>> renameTimeSeriesPaths = tree.getPipeRenameTimeSeriesPaths();
    Assert.assertEquals(1, renameTimeSeriesPaths.size());
    Assert.assertEquals(physicalPath.getFullPath(), renameTimeSeriesPaths.get(0).left);
    Assert.assertEquals(aliasPath.getFullPath(), renameTimeSeriesPaths.get(0).right);
  }

  private void assertPipeRenameTimeSeriesPlan(
      final List<Pair<PartialPath, PartialPath>> renameTimeSeriesList,
      final PartialPath oldPath,
      final PartialPath newPath) {
    Assert.assertEquals(1, renameTimeSeriesList.size());
    Assert.assertEquals(oldPath, renameTimeSeriesList.get(0).left);
    Assert.assertEquals(newPath, renameTimeSeriesList.get(0).right);
  }
}
