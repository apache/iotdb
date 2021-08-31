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

package org.apache.iotdb.db.engine.version;

import org.apache.iotdb.db.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SimpleFileVersionControllerTest {
  @Test
  public void test() throws IOException {
    String tempFilePath = TestConstant.BASE_OUTPUT_PATH.concat("version.tmp");

    try {
      if (!new File(tempFilePath).mkdir()) {
        Assert.fail("can not create version.tmp folder");
      }
      VersionController versionController = new SimpleFileVersionController(tempFilePath, 1);
      assertEquals(SimpleFileVersionController.getSaveInterval(), versionController.currVersion());
      for (int i = 0; i < 150; i++) {
        versionController.nextVersion();
      }
      assertEquals(
          SimpleFileVersionController.getSaveInterval() + 150, versionController.currVersion());
      versionController = new SimpleFileVersionController(tempFilePath, 1);
      assertEquals(
          SimpleFileVersionController.getSaveInterval() + 200, versionController.currVersion());
    } finally {
      FileUtils.deleteDirectory(new File(tempFilePath));
    }
  }
}
