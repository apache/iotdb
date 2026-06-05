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

package org.apache.iotdb.db.pipe.receiver.visitor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

public class PipeStatementTSStatusVisitorTest {

  @Test
  public void testClassifyObjectOffsetMismatchAsTemporaryUnavailable() {
    final PipeStatementTSStatusVisitor visitor = new PipeStatementTSStatusVisitor();

    final String objectOffsetMismatchMessage =
        String.format(DataNodeMiscMessages.OBJECT_FILE_LENGTH_NOT_EQUAL_TO_OFFSET, 0, 4101);
    final TSStatus status =
        visitor.process(
            new InsertTabletStatement(),
            new TSStatus(TSStatusCode.OBJECT_INSERT_ERROR.getStatusCode())
                .setMessage(objectOffsetMismatchMessage));

    Assert.assertEquals(
        TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
        status.getCode());
    Assert.assertEquals(objectOffsetMismatchMessage, status.getMessage());
  }
}
