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

package org.apache.iotdb.db.pipe.sink.util.cacher;

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
}
