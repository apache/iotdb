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

package org.apache.iotdb.commons.pipe.sink.payload.thrift.request;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PipeTransferFileSealReqV2Test {

  @Test
  public void testLegacyV13SnapshotSealCapturesTreeOnly() {
    final Map<String, String> parameters = new HashMap<>();

    Assert.assertTrue(PipeTransferFileSealReqV2.isTreeModelDataAllowedToBeCaptured(parameters));
    Assert.assertFalse(PipeTransferFileSealReqV2.isTableModelDataAllowedToBeCaptured(parameters));
  }

  @Test
  public void testExplicitTreeOnlySnapshotSealCapturesTreeOnly() {
    final Map<String, String> parameters = new HashMap<>();
    parameters.put(PipeTransferFileSealReqV2.TREE, "");

    Assert.assertTrue(PipeTransferFileSealReqV2.isTreeModelDataAllowedToBeCaptured(parameters));
    Assert.assertFalse(PipeTransferFileSealReqV2.isTableModelDataAllowedToBeCaptured(parameters));
  }

  @Test
  public void testExplicitTableOnlySnapshotSealCapturesTableOnly() {
    final Map<String, String> parameters = new HashMap<>();
    parameters.put(PipeTransferFileSealReqV2.TABLE, "");

    Assert.assertFalse(PipeTransferFileSealReqV2.isTreeModelDataAllowedToBeCaptured(parameters));
    Assert.assertTrue(PipeTransferFileSealReqV2.isTableModelDataAllowedToBeCaptured(parameters));
  }

  @Test
  public void testExplicitTreeAndTableSnapshotSealCapturesBoth() {
    final Map<String, String> parameters = new HashMap<>();
    parameters.put(PipeTransferFileSealReqV2.TREE, "");
    parameters.put(PipeTransferFileSealReqV2.TABLE, "");

    Assert.assertTrue(PipeTransferFileSealReqV2.isTreeModelDataAllowedToBeCaptured(parameters));
    Assert.assertTrue(PipeTransferFileSealReqV2.isTableModelDataAllowedToBeCaptured(parameters));
  }
}
