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

package org.apache.iotdb.db.pipe.sink.util;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LeaderCacheUtilsTest {

  @Test
  public void testParseRecommendedRedirectionsFromVariableStatementCount() {
    final TEndPoint redirectEndPoint = new TEndPoint("127.0.0.2", 6667);
    final TSStatus redirectedRowStatus =
        RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
            .setMessage("table1.device1")
            .setRedirectNode(redirectEndPoint);
    final TSStatus redirectedStatementStatus =
        RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)
            .setSubStatus(Collections.singletonList(redirectedRowStatus));
    final TSStatus batchStatus =
        RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)
            .setSubStatus(
                Arrays.asList(
                    RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS),
                    redirectedStatementStatus,
                    RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)));

    final List<Pair<String, TEndPoint>> redirects =
        LeaderCacheUtils.parseRecommendedRedirections(batchStatus);

    Assert.assertEquals(1, redirects.size());
    Assert.assertEquals("table1.device1", redirects.get(0).getLeft());
    Assert.assertEquals(redirectEndPoint, redirects.get(0).getRight());
  }

  @Test
  public void testParseRecommendedRedirectionsFromDirectMultiDeviceStatus() {
    final TEndPoint redirectEndPoint = new TEndPoint("127.0.0.3", 6667);
    final TSStatus directStatus =
        RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)
            .setSubStatus(
                Arrays.asList(
                    RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS),
                    RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
                        .setMessage("root.sg.device2")
                        .setRedirectNode(redirectEndPoint)));

    final List<Pair<String, TEndPoint>> redirects =
        LeaderCacheUtils.parseRecommendedRedirections(directStatus);

    Assert.assertEquals(
        Collections.singletonList(new Pair<>("root.sg.device2", redirectEndPoint)), redirects);
  }

  @Test
  public void testParseRecommendedRedirectionsIgnoresTopLevelRedirect() {
    final TSStatus status =
        RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)
            .setMessage("redirect recommendation")
            .setRedirectNode(new TEndPoint("127.0.0.4", 6667));

    Assert.assertTrue(LeaderCacheUtils.parseRecommendedRedirections(status).isEmpty());
    Assert.assertTrue(LeaderCacheUtils.parseRecommendedRedirections(null).isEmpty());
  }

  @Test
  public void testParseRecommendedRedirectionsIgnoresNonRedirectionStatus() {
    final TSStatus status =
        RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
            .setSubStatus(
                Collections.singletonList(
                    redirectStatus("root.sg.device3", new TEndPoint("127.0.0.5", 6667))));

    Assert.assertTrue(LeaderCacheUtils.parseRecommendedRedirections(status).isEmpty());
  }

  private static TSStatus redirectStatus(final String deviceId, final TEndPoint endPoint) {
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
        .setMessage(deviceId)
        .setRedirectNode(endPoint);
  }

  @Test
  public void testIgnoreRedirectsWithoutDevicePath() {
    final TEndPoint redirectEndPoint = new TEndPoint("127.0.0.2", 6667);
    final TSStatus tableRowWithoutPath =
        RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS).setRedirectNode(redirectEndPoint);
    final TSStatus rowWithEmptyPath =
        RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
            .setMessage("")
            .setRedirectNode(redirectEndPoint);
    final TSStatus treeRowWithPath =
        RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
            .setMessage("root.sg.d1")
            .setRedirectNode(redirectEndPoint);
    final TSStatus batchStatus =
        RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)
            .setSubStatus(
                Arrays.asList(
                    RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)
                        .setSubStatus(Arrays.asList(tableRowWithoutPath, rowWithEmptyPath)),
                    RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)
                        .setSubStatus(Collections.singletonList(treeRowWithPath))));

    final List<Pair<String, TEndPoint>> redirects =
        LeaderCacheUtils.parseRecommendedRedirections(batchStatus);

    Assert.assertEquals(1, redirects.size());
    Assert.assertEquals("root.sg.d1", redirects.get(0).getLeft());
    Assert.assertEquals(redirectEndPoint, redirects.get(0).getRight());
  }

  @Test
  public void testIgnoreMalformedRedirectStatus() {
    final TSStatus redirectWithoutSubStatus =
        RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND);
    final TSStatus batchStatus =
        RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)
            .setSubStatus(Arrays.asList(null, redirectWithoutSubStatus));

    Assert.assertTrue(LeaderCacheUtils.parseRecommendedRedirections(batchStatus).isEmpty());
    Assert.assertTrue(LeaderCacheUtils.parseRecommendedRedirections(null).isEmpty());
  }
}
