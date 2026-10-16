/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.confignode.consensus.request.write.function.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.DropFunctionPlan;
import org.apache.iotdb.udf.api.exception.UDFManagementException;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.utils.Binary;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class UDFInfoTest {

  private static final String SHARED_JAR_NAME = "shared.jar";
  private static final String SHARED_JAR_MD5 = "12345";
  private static final String DIFFERENT_JAR_MD5 = "54321";

  private static UDFInfo udfInfo;
  private static UDFInfo udfInfoSaveBefore;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setup() throws IOException {
    udfInfo = new UDFInfo();
    udfInfoSaveBefore = new UDFInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    udfInfo.clear();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testDropOneSharedJarReferenceKeepsJarMetadata()
      throws IOException, UDFManagementException {
    clearUdfInfos();

    udfInfo.addUDFInTable(createFunctionPlan("test1", SHARED_JAR_NAME, SHARED_JAR_MD5, true));
    udfInfo.addUDFInTable(createFunctionPlan("test2", SHARED_JAR_NAME, SHARED_JAR_MD5, false));

    udfInfo.dropFunction(new DropFunctionPlan("TEST1"));

    Assert.assertFalse(udfInfo.needToSaveJar(SHARED_JAR_NAME));
    Assert.assertEquals(1, udfInfo.getRawExistedJarToMD5().size());
    Assert.assertEquals(SHARED_JAR_MD5, udfInfo.getRawExistedJarToMD5().get(SHARED_JAR_NAME));

    udfInfo.validate("TEST3", SHARED_JAR_NAME, SHARED_JAR_MD5);
    try {
      udfInfo.validate("TEST3", SHARED_JAR_NAME, DIFFERENT_JAR_MD5);
      Assert.fail("Expected shared jar conflict after dropping only one referenced UDF.");
    } catch (UDFManagementException e) {
      Assert.assertTrue(e.getMessage().contains("different MD5"));
    }
  }

  @Test
  public void testSnapshotRebuildsSharedJarReferences() throws IOException {
    clearUdfInfos();
    FileUtils.cleanDirectory(snapshotDir);

    CreateFunctionPlan createFunctionPlan1 =
        createFunctionPlan("test1", SHARED_JAR_NAME, SHARED_JAR_MD5, true);
    CreateFunctionPlan createFunctionPlan2 =
        createFunctionPlan("test2", SHARED_JAR_NAME, SHARED_JAR_MD5, false);

    udfInfo.addUDFInTable(createFunctionPlan1);
    udfInfo.addUDFInTable(createFunctionPlan2);
    udfInfoSaveBefore.addUDFInTable(createFunctionPlan1);
    udfInfoSaveBefore.addUDFInTable(createFunctionPlan2);

    udfInfo.processTakeSnapshot(snapshotDir);
    udfInfo.clear();
    udfInfo.processLoadSnapshot(snapshotDir);

    Assert.assertEquals(udfInfoSaveBefore.getRawExistedJarToMD5(), udfInfo.getRawExistedJarToMD5());
    Assert.assertEquals(udfInfoSaveBefore.getRawUDFTable(), udfInfo.getRawUDFTable());

    udfInfo.dropFunction(new DropFunctionPlan("TEST1"));
    Assert.assertFalse(udfInfo.needToSaveJar(SHARED_JAR_NAME));
    Assert.assertEquals(SHARED_JAR_MD5, udfInfo.getRawExistedJarToMD5().get(SHARED_JAR_NAME));
  }

  private static void clearUdfInfos() {
    udfInfo.clear();
    udfInfoSaveBefore.clear();
  }

  private static CreateFunctionPlan createFunctionPlan(
      String functionName, String jarName, String jarMD5, boolean includeJarFile) {
    UDFInformation udfInformation =
        new UDFInformation(functionName, functionName, false, true, jarName, jarMD5);
    return new CreateFunctionPlan(
        udfInformation, includeJarFile ? new Binary(new byte[] {1, 2, 3}) : null);
  }
}
