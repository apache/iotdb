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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.mpp.rpc.thrift.TPushMultiPipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DataNodeInternalRPCServiceImplPushMultiPipeMetaTest {

  @Test
  public void testPushMultiPipeMetaContinuesAfterSinglePipeFailure() {
    final AtomicInteger pushedPipeCount = new AtomicInteger(0);
    final TPushPipeMetaResp resp =
        PushMultiPipeMetaHelper.pushMultiPipeMeta(
            new TPushMultiPipeMetaReq()
                .setPipeMetas(
                    Arrays.asList(
                        ByteBuffer.wrap(new byte[] {1}),
                        ByteBuffer.wrap(new byte[] {2}),
                        ByteBuffer.wrap(new byte[] {3}))),
            new PushMultiPipeMetaHelper.Handler() {
              @Override
              public TPushPipeMetaRespExceptionMessage handleDropPipe(final String pipeName) {
                Assert.fail("Unexpected drop pipe request");
                return null;
              }

              @Override
              public TPushPipeMetaRespExceptionMessage handleSinglePipeMeta(
                  final ByteBuffer pipeMeta) {
                final int index = pushedPipeCount.incrementAndGet();
                return index == 2 ? newExceptionMessage("pipe-2") : null;
              }
            });

    Assert.assertEquals(3, pushedPipeCount.get());
    Assert.assertEquals(
        TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode(), resp.getStatus().getCode());
    Assert.assertEquals(1, resp.getExceptionMessagesSize());
    Assert.assertEquals("pipe-2", resp.getExceptionMessages().get(0).getPipeName());
  }

  @Test
  public void testDropMultiPipeContinuesAfterSinglePipeFailure() {
    final List<String> droppedPipeNames = new ArrayList<>();
    final TPushPipeMetaResp resp =
        PushMultiPipeMetaHelper.pushMultiPipeMeta(
            new TPushMultiPipeMetaReq().setPipeNamesToDrop(Arrays.asList("pipe-1", "pipe-2")),
            new PushMultiPipeMetaHelper.Handler() {
              @Override
              public TPushPipeMetaRespExceptionMessage handleDropPipe(final String pipeName) {
                droppedPipeNames.add(pipeName);
                return "pipe-1".equals(pipeName) ? newExceptionMessage(pipeName) : null;
              }

              @Override
              public TPushPipeMetaRespExceptionMessage handleSinglePipeMeta(
                  final ByteBuffer pipeMeta) {
                Assert.fail("Unexpected pipe meta request");
                return null;
              }
            });

    Assert.assertEquals(Arrays.asList("pipe-1", "pipe-2"), droppedPipeNames);
    Assert.assertEquals(
        TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode(), resp.getStatus().getCode());
    Assert.assertEquals(1, resp.getExceptionMessagesSize());
    Assert.assertEquals("pipe-1", resp.getExceptionMessages().get(0).getPipeName());
  }

  @Test
  public void testPushMultiPipeMetaReturnsSuccessWithoutExceptionMessages() {
    final AtomicInteger pushedPipeCount = new AtomicInteger(0);
    final TPushPipeMetaResp resp =
        PushMultiPipeMetaHelper.pushMultiPipeMeta(
            new TPushMultiPipeMetaReq()
                .setPipeMetas(
                    Arrays.asList(
                        ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2}))),
            new PushMultiPipeMetaHelper.Handler() {
              @Override
              public TPushPipeMetaRespExceptionMessage handleDropPipe(final String pipeName) {
                Assert.fail("Unexpected drop pipe request");
                return null;
              }

              @Override
              public TPushPipeMetaRespExceptionMessage handleSinglePipeMeta(
                  final ByteBuffer pipeMeta) {
                pushedPipeCount.incrementAndGet();
                return null;
              }
            });

    Assert.assertEquals(2, pushedPipeCount.get());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
    Assert.assertFalse(resp.isSetExceptionMessages());
  }

  @Test
  public void testPushMultiPipeMetaKeepsCollectedExceptionMessagesWhenLaterPipeThrows() {
    final AtomicInteger pushedPipeCount = new AtomicInteger(0);
    final TPushPipeMetaResp resp =
        PushMultiPipeMetaHelper.pushMultiPipeMeta(
            new TPushMultiPipeMetaReq()
                .setPipeMetas(
                    Arrays.asList(
                        ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2}))),
            new PushMultiPipeMetaHelper.Handler() {
              @Override
              public TPushPipeMetaRespExceptionMessage handleDropPipe(final String pipeName) {
                Assert.fail("Unexpected drop pipe request");
                return null;
              }

              @Override
              public TPushPipeMetaRespExceptionMessage handleSinglePipeMeta(
                  final ByteBuffer pipeMeta) {
                final int index = pushedPipeCount.incrementAndGet();
                if (index == 1) {
                  return newExceptionMessage("pipe-1");
                }
                throw new RuntimeException("boom");
              }
            });

    Assert.assertEquals(2, pushedPipeCount.get());
    Assert.assertEquals(
        TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode(), resp.getStatus().getCode());
    Assert.assertEquals(1, resp.getExceptionMessagesSize());
    Assert.assertEquals("pipe-1", resp.getExceptionMessages().get(0).getPipeName());
  }

  @Test
  public void testDropMultiPipeMetaReturnsSuccessWithoutExceptionMessages() {
    final List<String> droppedPipeNames = new ArrayList<>();
    final TPushPipeMetaResp resp =
        PushMultiPipeMetaHelper.pushMultiPipeMeta(
            new TPushMultiPipeMetaReq().setPipeNamesToDrop(Arrays.asList("pipe-1", "pipe-2")),
            new PushMultiPipeMetaHelper.Handler() {
              @Override
              public TPushPipeMetaRespExceptionMessage handleDropPipe(final String pipeName) {
                droppedPipeNames.add(pipeName);
                return null;
              }

              @Override
              public TPushPipeMetaRespExceptionMessage handleSinglePipeMeta(
                  final ByteBuffer pipeMeta) {
                Assert.fail("Unexpected pipe meta request");
                return null;
              }
            });

    Assert.assertEquals(Arrays.asList("pipe-1", "pipe-2"), droppedPipeNames);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
    Assert.assertFalse(resp.isSetExceptionMessages());
  }

  @Test
  public void testInvalidPushMultiPipeMetaRequestReturnsErrorWithoutCallingHandler() {
    final TPushPipeMetaResp resp =
        PushMultiPipeMetaHelper.pushMultiPipeMeta(
            new TPushMultiPipeMetaReq(),
            new PushMultiPipeMetaHelper.Handler() {
              @Override
              public TPushPipeMetaRespExceptionMessage handleDropPipe(final String pipeName) {
                Assert.fail("Unexpected drop pipe request");
                return null;
              }

              @Override
              public TPushPipeMetaRespExceptionMessage handleSinglePipeMeta(
                  final ByteBuffer pipeMeta) {
                Assert.fail("Unexpected pipe meta request");
                return null;
              }
            });

    Assert.assertEquals(
        TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode(), resp.getStatus().getCode());
    Assert.assertEquals(0, resp.getExceptionMessagesSize());
  }

  @Test
  public void testPushMultiPipeMetaStopsOnUnexpectedException() {
    final AtomicInteger pushedPipeCount = new AtomicInteger(0);
    final TPushPipeMetaResp resp =
        PushMultiPipeMetaHelper.pushMultiPipeMeta(
            new TPushMultiPipeMetaReq()
                .setPipeMetas(
                    Arrays.asList(
                        ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2}))),
            new PushMultiPipeMetaHelper.Handler() {
              @Override
              public TPushPipeMetaRespExceptionMessage handleDropPipe(final String pipeName) {
                Assert.fail("Unexpected drop pipe request");
                return null;
              }

              @Override
              public TPushPipeMetaRespExceptionMessage handleSinglePipeMeta(
                  final ByteBuffer pipeMeta) {
                pushedPipeCount.incrementAndGet();
                throw new RuntimeException("boom");
              }
            });

    Assert.assertEquals(1, pushedPipeCount.get());
    Assert.assertEquals(
        TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode(), resp.getStatus().getCode());
    Assert.assertEquals(0, resp.getExceptionMessagesSize());
  }

  private static TPushPipeMetaRespExceptionMessage newExceptionMessage(final String pipeName) {
    return new TPushPipeMetaRespExceptionMessage(pipeName, "failed", 1L);
  }
}
