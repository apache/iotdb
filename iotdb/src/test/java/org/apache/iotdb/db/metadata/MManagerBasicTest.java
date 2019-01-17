/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MManagerBasicTest {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testAddPathAndExist() {

    MManager manager = MManager.getInstance();
    assertEquals(manager.pathExist("root"), true);

    assertEquals(manager.pathExist("root.laptop"), false);

    try {
      manager.setStorageLevelToMTree("root.laptop.d1");
    } catch (PathErrorException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.setStorageLevelToMTree("root.laptop");
    } catch (PathErrorException | IOException e) {
      Assert.assertEquals(
          "The seriesPath of root.laptop already exist, it can't be set to the storage group",
          e.getMessage());
    }

    try {
      manager.addPathToMTree("root.laptop.d1.s0", "INT32", "RLE", new String[0]);
    } catch (PathErrorException | MetadataArgsErrorException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertEquals(manager.pathExist("root.laptop"), true);
    assertEquals(manager.pathExist("root.laptop.d1"), true);
    assertEquals(manager.pathExist("root.laptop.d1.s0"), true);
    assertEquals(manager.pathExist("root.laptop.d1.s1"), false);
    try {
      manager.addPathToMTree("root.laptop.d1.s1", "INT32", "RLE", new String[0]);
    } catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }
    assertEquals(manager.pathExist("root.laptop.d1.s1"), true);
    try {
      manager.deletePathFromMTree("root.laptop.d1.s1");
    } catch (PathErrorException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    // just delete s0, and don't delete root.laptop.d1??
    // delete storage group or not
    assertEquals(manager.pathExist("root.laptop.d1.s1"), false);
    try {
      manager.deletePathFromMTree("root.laptop.d1.s0");
    } catch (PathErrorException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertEquals(manager.pathExist("root.laptop.d1.s0"), false);
    assertEquals(manager.pathExist("root.laptop.d1"), true);
    assertEquals(manager.pathExist("root.laptop"), true);
    assertEquals(manager.pathExist("root"), true);

    // can't delete the storage group

    // try {
    // manager.setStorageLevelToMTree("root.laptop");
    // } catch (PathErrorException | IOException e) {
    // fail(e.getMessage());
    // }
    try {
      manager.addPathToMTree("root.laptop.d1.s1", "INT32", "RLE", new String[0]);
    } catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.addPathToMTree("root.laptop.d1.s0", "INT32", "RLE", new String[0]);
    } catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    assertEquals(false, manager.pathExist("root.laptop.d2"));
    assertEquals(false, manager.checkFileNameByPath("root.laptop.d2"));

    try {
      manager.deletePathFromMTree("root.laptop.d1.s0");
    } catch (PathErrorException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      manager.deletePathFromMTree("root.laptop.d1.s1");
    } catch (PathErrorException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.setStorageLevelToMTree("root.laptop.d2");
    } catch (PathErrorException | IOException e) {
      Assert.assertEquals(
          String.format("The seriesPath of %s already exist, it can't be set to the storage group",
              "root.laptop.d2"),
          e.getMessage());
    }
    /*
     * check file level
     */
    assertEquals(manager.pathExist("root.laptop.d2.s1"), false);
    List<Path> paths = new ArrayList<>();
    paths.add(new Path("root.laptop.d2.s1"));
    try {
      manager.checkFileLevel(paths);
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.addPathToMTree("root.laptop.d2.s1", "INT32", "RLE", new String[0]);
    } catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.addPathToMTree("root.laptop.d2.s0", "INT32", "RLE", new String[0]);
    } catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.deletePathFromMTree("root.laptop.d2.s0");
    } catch (PathErrorException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      manager.deletePathFromMTree("root.laptop.d2.s1");
    } catch (PathErrorException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.addPathToMTree("root.laptop.d1.s0", "INT32", "RLE", new String[0]);
    } catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.addPathToMTree("root.laptop.d1.s1", "INT32", "RLE", new String[0]);
    } catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    paths = new ArrayList<>();
    paths.add(new Path("root.laptop.d1.s0"));
    try {
      manager.checkFileLevel(paths);
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.addPathToMTree("root.laptop.d1.s2", "INT32", "RLE", new String[0]);
    } catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }
    paths = new ArrayList<>();
    paths.add(new Path("root.laptop.d1.s2"));
    try {
      manager.checkFileLevel(paths);
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.addPathToMTree("root.laptop.d1.s3", "INT32", "RLE", new String[0]);
    } catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }
    paths = new ArrayList<>();
    paths.add(new Path("root.laptop.d1.s3"));
    try {
      manager.checkFileLevel(paths);
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
