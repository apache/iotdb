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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.procedure.impl.testonly.NeverFinishProcedure;

import org.apache.ratis.util.MD5FileUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.stream.LongStream;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class ProcedureInfoTest {
  private static final ProcedureInfo procedureInfo = new ProcedureInfo(null);
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setup() {
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() {
    if (snapshotDir.exists()) {
      FileUtils.deleteFileOrDirectory(snapshotDir);
    }
  }

  @Test
  public void testProcedureId() {
    final long lastProcedureId = 100;
    LongStream.range(0, lastProcedureId)
        .forEach(
            id ->
                procedureInfo.updateProcedure(
                    new UpdateProcedurePlan(new NeverFinishProcedure(id))));
    Assert.assertEquals(procedureInfo.getNextProcId(), lastProcedureId);
  }

  @Test
  public void testSnapshot() throws Exception {
    procedureInfo.updateProcedure(new UpdateProcedurePlan(new NeverFinishProcedure(1)));
    procedureInfo.updateProcedure(new UpdateProcedurePlan(new NeverFinishProcedure(100)));
    procedureInfo.updateProcedure(new UpdateProcedurePlan(new NeverFinishProcedure(99999)));
    Assert.assertTrue(procedureInfo.processTakeSnapshot(snapshotDir));
    ProcedureInfo procedureInfo1 = new ProcedureInfo(null);
    procedureInfo1.processLoadSnapshot(snapshotDir);
    Assert.assertEquals(procedureInfo, procedureInfo1);
  }

  @Test
  public void testSnapshotWithMd5Files() throws Exception {
    ProcedureInfo expected = new ProcedureInfo(null);
    expected.updateProcedure(new UpdateProcedurePlan(new NeverFinishProcedure(1)));
    expected.updateProcedure(new UpdateProcedurePlan(new NeverFinishProcedure(100)));
    assertSnapshotWithMd5Files(expected);
  }

  @Test
  public void testEmptySnapshotWithMd5File() throws Exception {
    assertSnapshotWithMd5Files(new ProcedureInfo(null));
  }

  private void assertSnapshotWithMd5Files(ProcedureInfo expected) throws Exception {
    File snapshotWithMd5 = temporaryFolder.newFolder();
    Assert.assertTrue(expected.processTakeSnapshot(snapshotWithMd5));
    File[] snapshotFiles = new File(snapshotWithMd5, "procedures").listFiles();
    Assert.assertNotNull(snapshotFiles);
    Assert.assertEquals(expected.getProcedures().size() + 1, snapshotFiles.length);
    for (File snapshotFile : snapshotFiles) {
      MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
    }

    ProcedureInfo actual = new ProcedureInfo(null);
    actual.processLoadSnapshot(snapshotWithMd5);
    Assert.assertEquals(expected, actual);

    for (File snapshotFile : snapshotFiles) {
      Assert.assertTrue(MD5FileUtil.getDigestFileForFile(snapshotFile).isFile());
      MD5FileUtil.verifySavedMD5(snapshotFile, MD5FileUtil.computeMd5ForFile(snapshotFile));
    }
  }
}
