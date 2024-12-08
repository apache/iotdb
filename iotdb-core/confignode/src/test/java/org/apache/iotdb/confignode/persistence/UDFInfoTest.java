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

import org.apache.iotdb.common.rpc.thrift.FunctionType;
import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.UDFType;
import org.apache.iotdb.confignode.consensus.request.write.function.CreateFunctionPlan;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.apache.tsfile.utils.Binary;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class UDFInfoTest {

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
  public void testSnapshot() throws TException, IOException, IllegalPathException {
    UDFInformation udfInformation =
        new UDFInformation(
            "test1",
            "test1",
            UDFType.of(Model.TREE, FunctionType.NONE, true),
            true,
            "test1.jar",
            "12345");
    CreateFunctionPlan createFunctionPlan =
        new CreateFunctionPlan(udfInformation, new Binary(new byte[] {1, 2, 3}));
    udfInfo.addUDFInTable(createFunctionPlan);
    udfInfoSaveBefore.addUDFInTable(createFunctionPlan);

    udfInformation =
        new UDFInformation(
            "test2",
            "test2",
            UDFType.of(Model.TREE, FunctionType.NONE, true),
            true,
            "test2.jar",
            "123456");
    createFunctionPlan = new CreateFunctionPlan(udfInformation, new Binary(new byte[] {1, 2, 3}));
    udfInfo.addUDFInTable(createFunctionPlan);
    udfInfoSaveBefore.addUDFInTable(createFunctionPlan);

    udfInfo.processTakeSnapshot(snapshotDir);
    udfInfo.clear();
    udfInfo.processLoadSnapshot(snapshotDir);

    Assert.assertEquals(udfInfoSaveBefore.getRawExistedJarToMD5(), udfInfo.getRawExistedJarToMD5());
    Assert.assertEquals(udfInfoSaveBefore.getRawUDFTable(), udfInfo.getRawUDFTable());
  }
}
