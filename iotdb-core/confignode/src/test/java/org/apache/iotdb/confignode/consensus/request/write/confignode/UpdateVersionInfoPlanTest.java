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

package org.apache.iotdb.confignode.consensus.request.write.confignode;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;

public class UpdateVersionInfoPlanTest {

  @Test
  public void capabilitiesRoundTrip() throws Exception {
    TNodeVersionInfo versionInfo =
        new TNodeVersionInfo("2.0.0", "build")
            .setSupportedCQDurationEncodingVersions(Collections.singleton((short) 1));
    UpdateVersionInfoPlan plan = new UpdateVersionInfoPlan(versionInfo, 3);
    UpdateVersionInfoPlan restored =
        (UpdateVersionInfoPlan) ConfigPhysicalPlan.Factory.create(plan.serializeToByteBuffer());
    Assert.assertEquals(plan, restored);
    Assert.assertTrue(
        restored.getVersionInfo().getSupportedCQDurationEncodingVersions().contains((short) 1));
  }

  @Test
  public void legacyPayloadWithoutCapabilityTailStillDeserializes() throws Exception {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(
          org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType.UpdateVersionInfo
              .getPlanType(),
          outputStream);
      ReadWriteIOUtils.write(7, outputStream);
      ReadWriteIOUtils.write("1.3.0", outputStream);
      ReadWriteIOUtils.write("legacy", outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      UpdateVersionInfoPlan restored =
          (UpdateVersionInfoPlan) ConfigPhysicalPlan.Factory.create(buffer);
      Assert.assertEquals(7, restored.getNodeId());
      Assert.assertEquals("1.3.0", restored.getVersionInfo().getVersion());
      Assert.assertFalse(restored.getVersionInfo().isSetSupportedCQDurationEncodingVersions());
    }
  }
}
