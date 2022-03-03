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

import org.apache.iotdb.db.pipe.external.operation.InsertOperation;
import org.apache.iotdb.db.pipe.external.operation.Operation;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TsfilePipeSrcEntryTest {

  @Test(timeout = 10_000L)
  public void testSingleReadEntry() throws IOException {
    String tsFilename = this.getClass().getClassLoader().getResource("test1.tsfile.data").getPath();
    TsfilePipeOpBlock tsfileDataSrcEntry = new TsfilePipeOpBlock("root", tsFilename, 0);

    assertEquals("root", tsfileDataSrcEntry.getStorageGroup());
    assertEquals(0, tsfileDataSrcEntry.getBeginIndex());
    assertEquals(6, tsfileDataSrcEntry.getDataCount());
    assertEquals(6, tsfileDataSrcEntry.getNextIndex());

    tsfileDataSrcEntry.setBeginIndex(2);
    assertEquals(8, tsfileDataSrcEntry.getNextIndex());

    Operation operation = null;
    for (int i = 0; i < tsfileDataSrcEntry.getDataCount(); i++) {
      operation = tsfileDataSrcEntry.getOperation(i + 2, 1);
      assertEquals("root", operation.getStorageGroup());
      assertEquals(1, operation.getDataCount());
      assertEquals(i + 2, operation.getStartIndex());
      assertEquals(i + 3, operation.getEndIndex());

      assertEquals(operation instanceof InsertOperation, true);
      InsertOperation insertOperation = (InsertOperation) operation;
      assertEquals(1, insertOperation.getDataList().size());
      // System.out.println("=== data" + i + ": " + operation);
    }

    InsertOperation insertOperation = (InsertOperation) operation;
    assertEquals(
        "root.lemming.device12.sensor1", insertOperation.getDataList().get(0).left.getFullPath());
    assertEquals(1617206403000L, insertOperation.getDataList().get(0).right.get(0).getTimestamp());
    assertEquals("22.0", insertOperation.getDataList().get(0).right.get(0).getValue().toString());

    for (int i = 0; i <= tsfileDataSrcEntry.getDataCount() - 3; i++) {
      operation = tsfileDataSrcEntry.getOperation(i + 2, 3);
      assertEquals("root", operation.getStorageGroup());
      assertEquals(3, operation.getDataCount());
      assertEquals(i + 2, operation.getStartIndex());
      assertEquals(i + 5, operation.getEndIndex());
      // System.out.println("=== data" + i + ": " + operation);
    }

    for (long i = 6; i < 8; i++) {
      operation = tsfileDataSrcEntry.getOperation(i, 3);
      assertEquals("root", operation.getStorageGroup());
      assertEquals(8 - i, operation.getDataCount());
      assertEquals(i, operation.getStartIndex());
      assertEquals(8, operation.getEndIndex());
      // System.out.println("=== data" + i + ": " + operation);
    }

    tsfileDataSrcEntry.close();
  }
}
