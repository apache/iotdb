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

package org.apache.iotdb.db.mpp.execution.fragment;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

public class FragmentInstanceFailureInfoSerdeTest {
  @Test
  public void testFragmentInstanceFailureInfoSerdeTest() throws IOException {
    // message is null, cause is null, lists are empty
    FragmentInstanceFailureInfo failureInfo =
        new FragmentInstanceFailureInfo(
            null, null, Collections.emptyList(), Collections.emptyList());
    ByteBuffer byteBuffer = failureInfo.serialize();
    FragmentInstanceFailureInfo res = FragmentInstanceFailureInfo.deserialize(byteBuffer);
    Assert.assertEquals(failureInfo, res);

    FragmentInstanceFailureInfo failureInfo1 =
        new FragmentInstanceFailureInfo(
            "testFragmentInstanceFailureInfo",
            null,
            Collections.emptyList(),
            Collections.singletonList("ERROR"));
    ByteBuffer byteBuffer1 = failureInfo1.serialize();
    FragmentInstanceFailureInfo res1 = FragmentInstanceFailureInfo.deserialize(byteBuffer1);
    Assert.assertEquals(failureInfo1, res1);
  }

  @Test
  public void testFragmentInstanceFailureInfoSerdeTest1() throws IOException {
    // message is null, cause is null, lists are empty
    FragmentInstanceFailureInfo failureInfo1 =
        new FragmentInstanceFailureInfo(
            null, null, Collections.emptyList(), Collections.emptyList());

    FragmentInstanceFailureInfo failureInfo2 =
        new FragmentInstanceFailureInfo(
            "testFragmentInstanceFailureInfo",
            null,
            Collections.emptyList(),
            Collections.singletonList("ERROR"));

    FragmentInstanceFailureInfo failureInfo3 =
        new FragmentInstanceFailureInfo(
            "test",
            failureInfo1,
            Collections.singletonList(failureInfo1),
            Collections.singletonList("test"));
    ByteBuffer byteBuffer3 = failureInfo3.serialize();
    FragmentInstanceFailureInfo res3 = FragmentInstanceFailureInfo.deserialize(byteBuffer3);
    Assert.assertEquals(failureInfo3, res3);

    FragmentInstanceFailureInfo failureInfo4 =
        new FragmentInstanceFailureInfo(
            "test",
            failureInfo2,
            Collections.singletonList(failureInfo1),
            Collections.singletonList("test"));
    ByteBuffer byteBuffer4 = failureInfo4.serialize();
    FragmentInstanceFailureInfo res4 = FragmentInstanceFailureInfo.deserialize(byteBuffer4);
    Assert.assertEquals(failureInfo4, res4);
  }
}
