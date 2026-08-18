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

package org.apache.iotdb.commons.pipe.datastructure;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.receiver.PipeReceiverStatusHandler;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE;

public class PipeReceiverStatusHandlerTest {

  @After
  public void tearDown() {
    PipeReceiverStatusHandler.setLogger(LoggerFactory.getLogger(PipeReceiverStatusHandler.class));
  }

  @Test
  public void testAuthLogger() {
    final Logger logger = Mockito.mock(Logger.class);
    Mockito.when(logger.isWarnEnabled()).thenReturn(true);
    PipeReceiverStatusHandler.setLogger(logger);

    final PipeReceiverStatusHandler handler = createHandler(true);
    final String recordMessage = "root.sg.d1.s1=1";

    handler.handle(
        new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode()),
        "",
        recordMessage);
    handler.handle(new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()), "", recordMessage);
    Mockito.verify(logger, Mockito.never())
        .warn(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any());

    handler.handle(
        new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()), "", recordMessage, true);
    handler.handle(
        new TSStatus(TSStatusCode.METADATA_ERROR.getStatusCode()),
        "No permissions for this operation, please add privilege WRITE_DATA",
        recordMessage,
        true);

    Mockito.verify(logger, Mockito.times(2))
        .warn(
            Mockito.anyString(),
            Mockito.eq("No permission"),
            Mockito.eq(recordMessage),
            Mockito.any(TSStatus.class));
  }

  @Test
  public void testGetPriorStatusReturnsHighestKnownPriority() {
    final List<TSStatus> statusList =
        Arrays.asList(
            status(TSStatusCode.SUCCESS_STATUS),
            status(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION),
            status(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION),
            status(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION));

    final TSStatus priorStatus = PipeReceiverStatusHandler.getPriorStatus(statusList);

    Assert.assertEquals(
        TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
        priorStatus.getCode());
    Assert.assertEquals(statusList, priorStatus.getSubStatus());
  }

  @Test
  public void testGetPriorStatusReturnsFirstUnknownStatus() {
    final TSStatus metadataError = status(TSStatusCode.METADATA_ERROR);

    Assert.assertSame(
        metadataError,
        PipeReceiverStatusHandler.getPriorStatus(
            Arrays.asList(
                status(TSStatusCode.SUCCESS_STATUS),
                metadataError,
                status(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION))));
  }

  private static PipeReceiverStatusHandler createHandler(final boolean skipIfNoPrivileges) {
    return new PipeReceiverStatusHandler(
        CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_DEFAULT_VALUE.equals("retry"),
        CONNECTOR_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE,
        CONNECTOR_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_DEFAULT_VALUE,
        CONNECTOR_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE,
        CONNECTOR_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_DEFAULT_VALUE,
        skipIfNoPrivileges);
  }

  private static TSStatus status(final TSStatusCode statusCode) {
    return new TSStatus(statusCode.getStatusCode());
  }
}
