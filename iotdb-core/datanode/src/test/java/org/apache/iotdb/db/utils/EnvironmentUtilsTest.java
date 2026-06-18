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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.utils.constant.TestConstant;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

public class EnvironmentUtilsTest {

  private final File testDir = new File(TestConstant.BASE_OUTPUT_PATH, "EnvironmentUtilsTest");

  @After
  public void tearDown() throws IOException {
    EnvironmentUtils.cleanDir(testDir.getPath());
  }

  @Test
  public void testCleanDirDeletesNestedDirectory() throws IOException {
    File nestedDir = new File(testDir, "ext" + File.separator + "udf" + File.separator + "tmp");
    Assert.assertTrue(nestedDir.isDirectory() || nestedDir.mkdirs());
    Files.write(new File(nestedDir, "plugin.txt").toPath(), Collections.singletonList("plugin"));

    EnvironmentUtils.cleanDir(testDir.getPath());

    Assert.assertFalse(testDir.exists());
  }
}
