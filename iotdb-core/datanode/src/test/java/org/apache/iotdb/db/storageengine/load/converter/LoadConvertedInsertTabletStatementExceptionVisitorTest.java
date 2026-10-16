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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class LoadConvertedInsertTabletStatementExceptionVisitorTest {

  @Test
  public void testPipeOutOfMemoryIsTemporaryUnavailable() throws Exception {
    final File tsFile = File.createTempFile("oom", ".tsfile");
    try {
      final LoadConvertedInsertTabletStatementExceptionVisitor visitor =
          new LoadConvertedInsertTabletStatementExceptionVisitor();
      final TSStatus status =
          visitor.visitLoadFile(
              LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()),
              new IllegalStateException(
                  "wrapped memory pressure",
                  new PipeRuntimeOutOfMemoryCriticalException("pipe tablet memory is not enough")));

      Assert.assertEquals(
          TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(), status.getCode());
      Assert.assertNotEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.getCode());
    } finally {
      Assert.assertTrue(tsFile.delete());
    }
  }
}
