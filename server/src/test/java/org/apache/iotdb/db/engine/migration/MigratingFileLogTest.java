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

package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;

import static org.apache.iotdb.db.metadata.idtable.IDTable.config;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MigratingFileLogTest {

  private static File MIGRATING_LOG_DIR =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(FilePathUtils.regularizePath(config.getSystemDir()), "migration", "migrating")
              .toString());

  @Test
  public void testWriteAndRead() throws Exception {
    // create test files
    File testTsfile = new File("test.tsfile");
    File testTsfileResource = new File("test.tsfile.resource");
    File testTsfileMods = new File("test.tsfile.mods");

    File testTargetDir = new File("testTargetDir");

    testTsfile.createNewFile();
    testTsfileResource.createNewFile();
    testTsfileMods.createNewFile();
    testTargetDir.mkdirs();

    // test write
    MigratingFileLogManager.getInstance().start(testTsfile, testTargetDir);

    File logFile =
        SystemFileFactory.INSTANCE.getFile(MIGRATING_LOG_DIR, testTsfile.getName() + ".log");
    FileInputStream fileInputStream = new FileInputStream(logFile);

    assertEquals(testTsfile.getAbsolutePath(), ReadWriteIOUtils.readString(fileInputStream));
    assertEquals(testTargetDir.getAbsolutePath(), ReadWriteIOUtils.readString(fileInputStream));

    fileInputStream.close();

    // test read
    MigratingFileLogManager.getInstance().recover();

    assertFalse(testTsfile.exists());
    assertFalse(testTsfileResource.exists());
    assertFalse(testTsfileMods.exists());

    assertFalse(logFile.exists());

    assertEquals(3, testTargetDir.listFiles().length);

    FileUtils.deleteDirectory(testTargetDir);
  }
}
