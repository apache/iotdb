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

package org.apache.iotdb.db.pipe.receiver.protocol.airgap;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapELanguageConstant;

import org.apache.tsfile.utils.BytesUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class IoTDBAirGapReceiverTest {

  @Test
  public void testRejectOversizedAirGapPayload() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final int originalMaxPayload = commonConfig.getPipeAirGapReceiverMaxPayloadSizeInBytes();

    try {
      commonConfig.setPipeAirGapReceiverMaxPayloadSizeInBytes(16);
      final IoTDBAirGapReceiver receiver = new IoTDBAirGapReceiver(new Socket(), 1L);

      final byte[] oversizedLength = BytesUtils.intToBytes(32);
      final InputStream inputStream =
          new ByteArrayInputStream(BytesUtils.concatByteArray(oversizedLength, oversizedLength));

      final IOException exception =
          Assert.assertThrows(IOException.class, () -> receiver.readData(inputStream));
      Assert.assertTrue(exception.getMessage().contains("payload length"));
    } finally {
      commonConfig.setPipeAirGapReceiverMaxPayloadSizeInBytes(originalMaxPayload);
    }
  }

  @Test
  public void testRejectNestedELanguagePrefix() throws Exception {
    final IoTDBAirGapReceiver receiver = new IoTDBAirGapReceiver(new Socket(), 2L);

    final InputStream inputStream =
        new ByteArrayInputStream(
            BytesUtils.concatByteArray(
                AirGapELanguageConstant.E_LANGUAGE_PREFIX,
                AirGapELanguageConstant.E_LANGUAGE_PREFIX));

    final IOException exception =
        Assert.assertThrows(IOException.class, () -> receiver.readData(inputStream));
    Assert.assertTrue(exception.getMessage().contains("nested E-Language prefix"));
  }
}
