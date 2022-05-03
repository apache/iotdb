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

package org.apache.iotdb.db.mpp.plan.plan;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.junit.Assert;
import org.junit.Test;

public class FragmentInstanceIdTest {
  @Test
  public void testFromThrift() {
    String queryId = "test_query";
    TFragmentInstanceId tId = new TFragmentInstanceId(queryId, 1, "0");
    FragmentInstanceId id = FragmentInstanceId.fromThrift(tId);
    Assert.assertEquals(queryId, id.getQueryId().getId());
    Assert.assertEquals(1, id.getFragmentId().getId());
    Assert.assertEquals("0", id.getInstanceId());
  }
}
