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
package org.apache.iotdb.db.engine.compaction.inner.sizetiered;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.task.FakedInnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.engine.storagegroup.FakedTsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SizeTieredCompactionSelectorTest {
  @Before
  public void setUp() {
    CompactionTaskManager.getInstance().start();
    new CompactionConfigRestorer().restoreCompactionConfig();
  }

  @After
  public void tearDown() {
    CompactionTaskManager.getInstance().stop();
    new CompactionConfigRestorer().restoreCompactionConfig();
  }

  @Test
  public void testSelectTieredTask() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(1800);
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(30);
    List<TsFileResource> resources = new ArrayList<>();
    int currentVersion = 1;
    for (int i = 0; i < 120; ++i) {
      resources.add(
          new FakedTsFileResource(
              1, String.format("%d-%d-0-0.tsfile", currentVersion, currentVersion)));
      currentVersion++;
    }
    for (int i = 0; i < 56; ++i) {
      resources.add(
          new FakedTsFileResource(
              30, String.format("%d-%d-1-0.tsfile", currentVersion, currentVersion)));
      currentVersion++;
    }
    for (int i = 0; i < 8; ++i) {
      resources.add(
          new FakedTsFileResource(
              900, String.format("%d-%d-2-0.tsfile", currentVersion, currentVersion)));
      currentVersion++;
    }
    TsFileManager tsFileManager = new TsFileManager("SizeTieredCompactionSelectorTask", "0");
    tsFileManager.addAll(resources, true);
    for (int i = 0; i < 5; i++) {
      SizeTieredCompactionSelector selector =
          new SizeTieredCompactionSelector(
              "SizeTieredCompactionSelectorTask",
              "0",
              0,
              tsFileManager,
              true,
              new FakedInnerSpaceCompactionTaskFactory());
      selector.selectAndSubmit();
      CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
      Thread.sleep(50);
    }
    List<TsFileResource> newResourceList = tsFileManager.getTsFileList(true);
    Assert.assertEquals(5, newResourceList.size());
    for (TsFileResource tsFileResource : newResourceList) {
      Assert.assertEquals(3, getCompactionCountOfResource(tsFileResource));
    }
  }

  @Test
  public void testSelectSandwichTask1() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(1800);
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(30);
    List<TsFileResource> resources = new ArrayList<>();
    int currentVersion = 1;
    resources.add(
        new FakedTsFileResource(
            1, String.format("%d-%d-0-0.tsfile", currentVersion, currentVersion)));
    currentVersion++;
    for (int i = 0; i < 10; ++i) {
      resources.add(
          new FakedTsFileResource(
              30, String.format("%d-%d-1-0.tsfile", currentVersion, currentVersion)));
      currentVersion++;
    }
    for (int i = 0; i < 8; ++i) {
      resources.add(
          new FakedTsFileResource(
              900, String.format("%d-%d-2-0.tsfile", currentVersion, currentVersion)));
      currentVersion++;
    }
    TsFileManager tsFileManager = new TsFileManager("SizeTieredCompactionSelectorTask", "0");
    tsFileManager.addAll(resources, true);
    for (int i = 0; i < 2; i++) {
      SizeTieredCompactionSelector selector =
          new SizeTieredCompactionSelector(
              "SizeTieredCompactionSelectorTask",
              "0",
              0,
              tsFileManager,
              true,
              new FakedInnerSpaceCompactionTaskFactory());
      selector.selectAndSubmit();
      CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
      List<TsFileResource> temp = tsFileManager.getTsFileList(true);
      Thread.sleep(50);
    }
    List<TsFileResource> newResourceList = tsFileManager.getTsFileList(true);
    Assert.assertEquals(4, newResourceList.size());
    for (TsFileResource tsFileResource : newResourceList) {
      Assert.assertEquals(3, getCompactionCountOfResource(tsFileResource));
    }
  }

  @Test
  public void testSelectSandwichTask2() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(1800);
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(30);
    List<TsFileResource> resources = new ArrayList<>();
    int currentVersion = 1;
    for (int i = 0; i < 10; ++i) {
      resources.add(
          new FakedTsFileResource(
              1, String.format("%d-%d-0-0.tsfile", currentVersion, currentVersion)));
      currentVersion++;
    }
    for (int i = 0; i < 10; ++i) {
      resources.add(
          new FakedTsFileResource(
              30, String.format("%d-%d-1-0.tsfile", currentVersion, currentVersion)));
      currentVersion++;
    }
    for (int i = 0; i < 8; ++i) {
      resources.add(
          new FakedTsFileResource(
              900, String.format("%d-%d-2-0.tsfile", currentVersion, currentVersion)));
      currentVersion++;
    }
    TsFileManager tsFileManager = new TsFileManager("SizeTieredCompactionSelectorTask", "0");
    tsFileManager.addAll(resources, true);

    SizeTieredCompactionSelector selector =
        new SizeTieredCompactionSelector(
            "SizeTieredCompactionSelectorTask",
            "0",
            0,
            tsFileManager,
            true,
            new FakedInnerSpaceCompactionTaskFactory());
    selector.selectAndSubmit();
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    Thread.sleep(50);
    List<TsFileResource> newResourceList = tsFileManager.getTsFileList(true);
    Assert.assertEquals(19, newResourceList.size());
    for (int i = 0; i < newResourceList.size(); ++i) {
      if (i < 11) {
        Assert.assertEquals(1, getCompactionCountOfResource(newResourceList.get(i)));
      } else {
        Assert.assertEquals(2, getCompactionCountOfResource(newResourceList.get(i)));
      }
    }

    selector =
        new SizeTieredCompactionSelector(
            "SizeTieredCompactionSelectorTask",
            "0",
            0,
            tsFileManager,
            true,
            new FakedInnerSpaceCompactionTaskFactory());
    selector.selectAndSubmit();
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    Thread.sleep(50);

    newResourceList = tsFileManager.getTsFileList(true);
    Assert.assertEquals(9, newResourceList.size());
    for (int i = 0; i < newResourceList.size(); ++i) {
      Assert.assertEquals(2, getCompactionCountOfResource(newResourceList.get(i)));
    }

    selector =
        new SizeTieredCompactionSelector(
            "SizeTieredCompactionSelectorTask",
            "0",
            0,
            tsFileManager,
            true,
            new FakedInnerSpaceCompactionTaskFactory());
    selector.selectAndSubmit();
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    Thread.sleep(50);

    newResourceList = tsFileManager.getTsFileList(true);
    Assert.assertEquals(4, newResourceList.size());
    for (int i = 0; i < newResourceList.size(); ++i) {
      Assert.assertEquals(3, getCompactionCountOfResource(newResourceList.get(i)));
    }

    // this selection will not select any task
    selector =
        new SizeTieredCompactionSelector(
            "SizeTieredCompactionSelectorTask",
            "0",
            0,
            tsFileManager,
            true,
            new FakedInnerSpaceCompactionTaskFactory());
    selector.selectAndSubmit();
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    Thread.sleep(50);

    newResourceList = tsFileManager.getTsFileList(true);
    Assert.assertEquals(4, newResourceList.size());
    for (int i = 0; i < newResourceList.size(); ++i) {
      Assert.assertEquals(3, getCompactionCountOfResource(newResourceList.get(i)));
    }
  }

  @Test
  public void testSelectSandwichTask3() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setTargetCompactionFileSize(1800);
    IoTDBDescriptor.getInstance().getConfig().setMaxCompactionCandidateFileNum(30);
    List<TsFileResource> resources = new ArrayList<>();
    int currentVersion = 1;
    for (int i = 0; i < 10; ++i) {
      resources.add(
          new FakedTsFileResource(
              30, String.format("%d-%d-1-0.tsfile", currentVersion, currentVersion)));
      currentVersion++;
    }
    resources.add(
        new FakedTsFileResource(
            1, String.format("%d-%d-0-0.tsfile", currentVersion, currentVersion)));
    currentVersion++;
    for (int i = 0; i < 8; ++i) {
      resources.add(
          new FakedTsFileResource(
              900, String.format("%d-%d-2-0.tsfile", currentVersion, currentVersion)));
      currentVersion++;
    }
    TsFileManager tsFileManager = new TsFileManager("SizeTieredCompactionSelectorTask", "0");
    tsFileManager.addAll(resources, true);
    SizeTieredCompactionSelector selector =
        new SizeTieredCompactionSelector(
            "SizeTieredCompactionSelectorTask",
            "0",
            0,
            tsFileManager,
            true,
            new FakedInnerSpaceCompactionTaskFactory());
    selector.selectAndSubmit();
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    Thread.sleep(50);
    List<TsFileResource> newResourceList = tsFileManager.getTsFileList(true);
    Assert.assertEquals(17, newResourceList.size());

    selector =
        new SizeTieredCompactionSelector(
            "SizeTieredCompactionSelectorTask",
            "0",
            0,
            tsFileManager,
            true,
            new FakedInnerSpaceCompactionTaskFactory());
    selector.selectAndSubmit();
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    Thread.sleep(50);
    newResourceList = tsFileManager.getTsFileList(true);
    Assert.assertEquals(8, newResourceList.size());

    selector =
        new SizeTieredCompactionSelector(
            "SizeTieredCompactionSelectorTask",
            "0",
            0,
            tsFileManager,
            true,
            new FakedInnerSpaceCompactionTaskFactory());
    selector.selectAndSubmit();
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    Thread.sleep(50);
    newResourceList = tsFileManager.getTsFileList(true);
    Assert.assertEquals(7, newResourceList.size());

    selector =
        new SizeTieredCompactionSelector(
            "SizeTieredCompactionSelectorTask",
            "0",
            0,
            tsFileManager,
            true,
            new FakedInnerSpaceCompactionTaskFactory());
    selector.selectAndSubmit();
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    Thread.sleep(50);
    newResourceList = tsFileManager.getTsFileList(true);
    Assert.assertEquals(4, newResourceList.size());

    Assert.assertEquals(4, getCompactionCountOfResource(newResourceList.get(0)));
    for (int i = 1; i < newResourceList.size(); ++i) {
      Assert.assertEquals(3, getCompactionCountOfResource(newResourceList.get(i)));
    }

    // this selection will not select any task
    selector =
        new SizeTieredCompactionSelector(
            "SizeTieredCompactionSelectorTask",
            "0",
            0,
            tsFileManager,
            true,
            new FakedInnerSpaceCompactionTaskFactory());
    selector.selectAndSubmit();
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
    Thread.sleep(50);
    newResourceList = tsFileManager.getTsFileList(true);
    Assert.assertEquals(4, newResourceList.size());

    Assert.assertEquals(4, getCompactionCountOfResource(newResourceList.get(0)));
    for (int i = 1; i < newResourceList.size(); ++i) {
      Assert.assertEquals(3, getCompactionCountOfResource(newResourceList.get(i)));
    }
  }

  private int getCompactionCountOfResource(TsFileResource resource) throws IOException {
    TsFileNameGenerator.TsFileName name =
        TsFileNameGenerator.getTsFileName(resource.getTsFile().getName());
    return name.getInnerCompactionCnt();
  }
}
