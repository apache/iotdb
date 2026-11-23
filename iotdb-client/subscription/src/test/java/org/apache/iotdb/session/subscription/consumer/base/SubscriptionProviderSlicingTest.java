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

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestType;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestVersion;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeSliceReq;
import org.apache.iotdb.rpc.subscription.payload.request.SubscriptionHeartbeatReq;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseType;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseVersion;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SubscriptionProviderSlicingTest {

  @Test
  public void testSendSlicesInOrderAndReturnFinalResponse() throws Exception {
    final TPipeSubscribeReq req = request(new byte[] {0, 1, 2, 3, 4, 5, 6});
    final List<TPipeSubscribeReq> sentRequests = new ArrayList<>();
    final TPipeSubscribeResp finalResp = response(TSStatusCode.SUCCESS_STATUS);

    final TPipeSubscribeResp actualResp =
        AbstractSubscriptionProvider.pipeSubscribeWithOptionalSlicing(
            req,
            3,
            sentReq -> {
              sentRequests.add(sentReq);
              return sentRequests.size() == 3 ? finalResp : response(TSStatusCode.SUCCESS_STATUS);
            });

    Assert.assertSame(finalResp, actualResp);
    Assert.assertEquals(3, sentRequests.size());
    Integer orderId = null;
    for (int sliceIndex = 0; sliceIndex < sentRequests.size(); sliceIndex++) {
      final PipeSubscribeSliceReq slice =
          PipeSubscribeSliceReq.fromTPipeSubscribeReq(sentRequests.get(sliceIndex));
      if (orderId == null) {
        orderId = slice.getOrderId();
      }
      Assert.assertEquals(orderId.intValue(), slice.getOrderId());
      Assert.assertEquals(sliceIndex, slice.getSliceIndex());
      Assert.assertEquals(sentRequests.size(), slice.getSliceCount());
    }
  }

  @Test
  public void testStopSendingAfterIntermediateSliceError() throws Exception {
    final TPipeSubscribeReq req = request(new byte[] {0, 1, 2, 3, 4});
    final List<TPipeSubscribeReq> sentRequests = new ArrayList<>();
    final TPipeSubscribeResp errorResp = response(TSStatusCode.SUBSCRIPTION_TYPE_ERROR);

    final TPipeSubscribeResp actualResp =
        AbstractSubscriptionProvider.pipeSubscribeWithOptionalSlicing(
            req,
            2,
            sentReq -> {
              sentRequests.add(sentReq);
              return sentRequests.size() == 2 ? errorResp : response(TSStatusCode.SUCCESS_STATUS);
            });

    Assert.assertSame(errorResp, actualResp);
    Assert.assertEquals(2, sentRequests.size());
  }

  @Test
  public void testSendSmallRequestWithoutWrapping() throws Exception {
    final TPipeSubscribeReq req = request(new byte[] {0, 1, 2});
    final List<TPipeSubscribeReq> sentRequests = new ArrayList<>();
    final TPipeSubscribeResp expectedResp = response(TSStatusCode.SUCCESS_STATUS);

    final TPipeSubscribeResp actualResp =
        AbstractSubscriptionProvider.pipeSubscribeWithOptionalSlicing(
            req,
            3,
            sentReq -> {
              sentRequests.add(sentReq);
              return expectedResp;
            });

    Assert.assertSame(expectedResp, actualResp);
    Assert.assertEquals(Collections.singletonList(req), sentRequests);
  }

  @Test
  public void testRejectOversizedUnsliceableRequest() {
    for (final PipeSubscribeRequestType type :
        new PipeSubscribeRequestType[] {
          PipeSubscribeRequestType.HANDSHAKE, PipeSubscribeRequestType.CLOSE
        }) {
      final TPipeSubscribeReq req = request(new byte[] {0, 1, 2, 3});
      req.type = type.getType();
      Assert.assertThrows(
          TException.class,
          () ->
              AbstractSubscriptionProvider.pipeSubscribeWithOptionalSlicing(
                  req,
                  3,
                  ignored -> {
                    Assert.fail();
                    return null;
                  }));
    }
    Assert.assertThrows(
        TException.class,
        () ->
            AbstractSubscriptionProvider.pipeSubscribeWithOptionalSlicing(
                request(new byte[] {0}),
                0,
                ignored -> {
                  Assert.fail();
                  return null;
                }));
  }

  @Test
  public void testPartitionCommitContextsBySerializedSize() throws Exception {
    final SubscriptionCommitContext first = commitContext("a", 1);
    final SubscriptionCommitContext second = commitContext("bb", 2);
    final SubscriptionCommitContext third = commitContext("ccc", 3);
    final int fixedBodySize = Integer.BYTES;
    final int maxBodySize =
        fixedBodySize
            + SubscriptionCommitContext.serialize(first).remaining()
            + SubscriptionCommitContext.serialize(second).remaining();

    final List<List<SubscriptionCommitContext>> batches =
        AbstractSubscriptionProvider.partitionCommitContexts(
            Arrays.asList(first, second, third), maxBodySize, fixedBodySize);

    Assert.assertEquals(2, batches.size());
    Assert.assertEquals(Arrays.asList(first, second), batches.get(0));
    Assert.assertEquals(Collections.singletonList(third), batches.get(1));
    for (final List<SubscriptionCommitContext> batch : batches) {
      Assert.assertTrue(
          SubscriptionHeartbeatReq.toThriftReq(batch).body.remaining() <= maxBodySize);
    }
    Assert.assertEquals(
        Collections.singletonList(Collections.emptyList()),
        AbstractSubscriptionProvider.partitionCommitContexts(
            Collections.emptyList(), maxBodySize, fixedBodySize));
  }

  @Test
  public void testMergeTopicProgressAcrossBatches() {
    final RegionProgress firstRegionProgress = regionProgress("1_1", 1, 1);
    final RegionProgress updatedFirstRegionProgress = regionProgress("1_1", 1, 2);
    final RegionProgress secondRegionProgress = regionProgress("1_2", 2, 3);
    final Map<String, RegionProgress> initialRegions = new LinkedHashMap<>();
    initialRegions.put("1_1", firstRegionProgress);
    final Map<String, TopicProgress> target = new LinkedHashMap<>();
    target.put("topic", new TopicProgress(initialRegions));

    final Map<String, RegionProgress> newRegions = new LinkedHashMap<>();
    newRegions.put("1_1", updatedFirstRegionProgress);
    newRegions.put("1_2", secondRegionProgress);
    final Map<String, TopicProgress> source = new LinkedHashMap<>();
    source.put("topic", new TopicProgress(newRegions));
    source.put(
        "another-topic", new TopicProgress(Collections.singletonMap("2_1", secondRegionProgress)));

    AbstractSubscriptionProvider.mergeTopicProgress(target, source);

    Assert.assertEquals(2, target.size());
    Assert.assertEquals(
        updatedFirstRegionProgress, target.get("topic").getRegionProgress().get("1_1"));
    Assert.assertEquals(secondRegionProgress, target.get("topic").getRegionProgress().get("1_2"));
    Assert.assertEquals(source.get("another-topic"), target.get("another-topic"));
  }

  private static TPipeSubscribeReq request(final byte[] body) {
    final TPipeSubscribeReq req = new TPipeSubscribeReq();
    req.version = PipeSubscribeRequestVersion.VERSION_1.getVersion();
    req.type = PipeSubscribeRequestType.POLL.getType();
    req.body = ByteBuffer.wrap(body);
    return req;
  }

  private static TPipeSubscribeResp response(final TSStatusCode statusCode) {
    return new TPipeSubscribeResp(
        new TSStatus(statusCode.getStatusCode()),
        PipeSubscribeResponseVersion.VERSION_1.getVersion(),
        PipeSubscribeResponseType.ACK.getType());
  }

  private static SubscriptionCommitContext commitContext(
      final String topicName, final long commitId) {
    return new SubscriptionCommitContext(1, 1, topicName, "group", commitId);
  }

  private static RegionProgress regionProgress(
      final String regionId, final int nodeId, final long localSequence) {
    return new RegionProgress(
        Collections.singletonMap(
            new WriterId(regionId, nodeId), new WriterProgress(1L, localSequence)));
  }
}
