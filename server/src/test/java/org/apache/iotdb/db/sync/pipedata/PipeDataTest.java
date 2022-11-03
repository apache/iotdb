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
package org.apache.iotdb.db.sync.pipedata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.modification.Deletion;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class PipeDataTest {
  private static final Logger logger = LoggerFactory.getLogger(PipeDataTest.class);
  private static final String pipeLogPath = "target/pipelog";

  @Test
  public void testSerializeAndDeserialize() {
    try {
      File f1 = new File(pipeLogPath);
      File f2 = new File(pipeLogPath);
      PipeData pipeData1 = new TsFilePipeData("1", 1);
      Deletion deletion = new Deletion(new PartialPath("root.sg1.d1.s1"), 0, 1, 5);
      PipeData pipeData2 = new DeletionPipeData(deletion, 3);
      DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(f2));
      pipeData1.serialize(outputStream);
      outputStream.flush();
      DataInputStream inputStream = new DataInputStream(new FileInputStream(f1));
      Assert.assertEquals(pipeData1, PipeData.createPipeData(inputStream));
      pipeData2.serialize(outputStream);
      outputStream.flush();
      Assert.assertEquals(pipeData2, PipeData.createPipeData(inputStream));
      inputStream.close();
      outputStream.close();

      Assert.assertEquals(pipeData1, PipeData.createPipeData(pipeData1.serialize()));
      Assert.assertEquals(pipeData2, PipeData.createPipeData(pipeData2.serialize()));
    } catch (Exception e) {
      logger.error(e.getMessage());
      Assert.fail();
    }
  }
}
