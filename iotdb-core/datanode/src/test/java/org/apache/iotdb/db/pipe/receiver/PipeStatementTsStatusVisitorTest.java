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

package org.apache.iotdb.db.pipe.receiver;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.db.pipe.receiver.protocol.thrift.IoTDBDataNodeReceiver;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;

public class PipeStatementTsStatusVisitorTest {

  @Test
  public void testActivateTemplate() {
    Assert.assertEquals(
        TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode(),
        IoTDBDataNodeReceiver.STATEMENT_STATUS_VISITOR
            .process(
                new BatchActivateTemplateStatement(Collections.emptyList()),
                new TSStatus(TSStatusCode.MULTIPLE_ERROR.getStatusCode())
                    .setSubStatus(
                        Arrays.asList(
                            StatusUtils.OK,
                            new TSStatus(TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()))))
            .getCode());
  }

  @Test
  public void testTTLIdempotency() {
    Assert.assertEquals(
        TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode(),
        IoTDBDataNodeReceiver.STATEMENT_STATUS_VISITOR
            .process(
                new InsertRowsStatement(),
                new TSStatus(TSStatusCode.MULTIPLE_ERROR.getStatusCode())
                    .setSubStatus(
                        Arrays.asList(
                            StatusUtils.OK, new TSStatus(TSStatusCode.OUT_OF_TTL.getStatusCode()))))
            .getCode());
  }

  @Test
  public void testLoadTemporaryUnavailableClassification() throws Exception {
    final File tsFile = File.createTempFile("temporary-unavailable", ".tsfile");
    tsFile.deleteOnExit();

    Assert.assertEquals(
        TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
        IoTDBDataNodeReceiver.STATEMENT_STATUS_VISITOR
            .process(
                LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()),
                new TSStatus(TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
                    .setMessage("schema is not ready"))
            .getCode());
  }

  @Test
  public void testLoadFileErrorWithMemoryMessageIsNotClassifiedByMessage() throws Exception {
    final File tsFile = File.createTempFile("memory-error", ".tsfile");
    tsFile.deleteOnExit();

    Assert.assertEquals(
        TSStatusCode.LOAD_FILE_ERROR.getStatusCode(),
        IoTDBDataNodeReceiver.STATEMENT_STATUS_VISITOR
            .process(
                LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()),
                new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
                    .setMessage("memory pressure"))
            .getCode());
  }

  @Test
  public void testDatabaseNotExistRuntimeExceptionClassification() {
    Assert.assertEquals(
        TSStatusCode.PIPE_RECEIVER_PARALLEL_OR_USER_CONFLICT_EXCEPTION.getStatusCode(),
        IoTDBDataNodeReceiver.STATEMENT_EXCEPTION_VISITOR
            .process(
                new InsertRowsStatement(),
                new IoTDBRuntimeException(
                    "Create DataPartition failed because the database: root.test.sg_0 is not exists",
                    TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()))
            .getCode());
  }
}
