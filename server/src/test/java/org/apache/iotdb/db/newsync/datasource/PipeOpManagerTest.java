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

package org.apache.iotdb.db.newsync.datasource;

import org.apache.iotdb.db.pipe.external.operation.Operation;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class PipeOpManagerTest {

  @Test(timeout = 10_000L)
  public void testPipSrcManager() throws IOException {
    PipeOpManager pipeOpManager = new PipeOpManager(null);

    String sgName1 = "root1";
    String sgName2 = "root2";
    String tsFilename1 =
        this.getClass().getClassLoader().getResource("test1.tsfile.data").getPath();
    String tsFilename2 =
        this.getClass().getClassLoader().getResource("test2.unseq.tsfile.data").getPath();

    TsfilePipeOpBlock tsfileDataSrcEntry1 = new TsfilePipeOpBlock(sgName1, tsFilename1, 1);
    pipeOpManager.appendDataSrc(sgName1, tsfileDataSrcEntry1);
    TsfilePipeOpBlock tsfileDataSrcEntry2 = new TsfilePipeOpBlock(sgName2, tsFilename2, 2);
    pipeOpManager.appendDataSrc(sgName2, tsfileDataSrcEntry2);

    long count1 = tsfileDataSrcEntry1.getDataCount();
    assertEquals(count1, 6);
    for (int i = 0; i < count1; i++) {
      Operation operation = pipeOpManager.getOperation(sgName1, i, 8);
      System.out.println("=== data" + i + ": " + operation + ", ");
      assertEquals("root1", operation.getStorageGroup());
    }

    Operation operation = pipeOpManager.getOperation(sgName1, 0, 18);
    System.out.println("+++ data10" + ": " + operation + ", ");

    pipeOpManager.commitData(sgName1, count1 - 1);
    operation = pipeOpManager.getOperation(sgName1, 7, 18);
    System.out.println("+++ data11" + ": " + operation + ", ");
    assertEquals(operation, null);

    operation = pipeOpManager.getOperation(sgName2, 6, 18);
    System.out.println("+++ data12" + ": " + operation + ", ");
    assertEquals(operation.getDataCount(), 4);
  }
}
