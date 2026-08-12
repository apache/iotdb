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

package com.timecho.iotdb.commons.commission;

import org.apache.iotdb.commons.conf.Edition;
import org.apache.iotdb.commons.exception.LicenseException;
import org.apache.iotdb.commons.i18n.CommissionMessages;

import com.timecho.iotdb.commons.commission.complete.HmacProtocol;
import com.timecho.iotdb.commons.external.codec.binary.Base32;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class BanditTest {

  @Test
  public void testV02DecryptUsesClearedHardwareAsSaltWhenHardwareCheckIsSkipped() throws Exception {
    List<String> currentSystemInfo =
        Arrays.asList("02-AAAAAAAA-PJPLSTKB-6VOVROH5", "02-BBBBBBBB-12345678-ABCDEFGH");
    String clearedSystemInfo = "02-00000000-PJPLSTKB-6VOVROH5,02-00000000-12345678-ABCDEFGH";
    String activationCode = buildV02ActivationCode(clearedSystemInfo, true);

    Properties properties = decryptV02(activationCode, currentSystemInfo);

    Assert.assertEquals(
        Boolean.TRUE.toString(),
        properties.getProperty(Lottery.SKIP_HARDWARE_SYSTEM_INFO_CHECK_NAME));
    Assert.assertEquals(clearedSystemInfo, properties.getProperty(Lottery.SYSTEM_INFO_HASH));
  }

  @Test
  public void testV02DecryptKeepsHardwareInSaltWhenHardwareCheckIsNotSkipped() throws Exception {
    List<String> systemInfo = Arrays.asList("02-AAAAAAAA-PJPLSTKB-6VOVROH5");
    String activationCode = buildV02ActivationCode(systemInfo.get(0), false);

    Properties properties = decryptV02(activationCode, systemInfo);

    Assert.assertEquals(systemInfo.get(0), properties.getProperty(Lottery.SYSTEM_INFO_HASH));
  }

  @Test
  public void testV02DecryptRejectsHardwareChangeWhenHardwareCheckIsNotSkipped() throws Exception {
    String activationCode = buildV02ActivationCode("02-AAAAAAAA-PJPLSTKB-6VOVROH5", false);

    LicenseException exception =
        Assert.assertThrows(
            LicenseException.class,
            () -> decryptV02(activationCode, Arrays.asList("02-BBBBBBBB-PJPLSTKB-6VOVROH5")));

    Assert.assertEquals(
        CommissionMessages.EXCEPTION_ILLEGAL_LICENSE_9E683B8A, exception.getMessage());
  }

  @Test
  public void testV02DecryptReadsReleaseByteAfterExistingFields() throws Exception {
    String systemInfo = "02-AAAAAAAA-PJPLSTKB-6VOVROH5";
    String clearedSystemInfo = "02-00000000-PJPLSTKB-6VOVROH5";
    byte identifier = (byte) ((1 << 4) | (1 << 5));
    ByteBuffer dataBuffer = ByteBuffer.allocate(7);
    dataBuffer.put(identifier);
    dataBuffer.putInt(20300101);
    dataBuffer.put((byte) 1);
    dataBuffer.put((byte) Edition.IOTDB.getRelease());
    byte[] data = dataBuffer.array();
    byte[] tag = HmacProtocol.calculateTag(data, clearedSystemInfo);
    ByteBuffer payloadBuffer = ByteBuffer.allocate(data.length + tag.length);
    payloadBuffer.put(data);
    payloadBuffer.put(tag);

    Properties properties =
        decryptV02(new Base32().encodeAsString(payloadBuffer.array()), Arrays.asList(systemInfo));

    Assert.assertEquals(
        String.valueOf(Edition.IOTDB.getRelease()),
        properties.getProperty(Lottery.PRODUCT_RELEASE_NAME));
  }

  private static String buildV02ActivationCode(String salt, boolean skipHardwareCheck)
      throws Exception {
    byte identifier = (byte) (skipHardwareCheck ? (1 << 4) : 0);
    ByteBuffer dataBuffer = ByteBuffer.allocate(skipHardwareCheck ? 6 : 5);
    dataBuffer.put(identifier);
    dataBuffer.putInt(20300101);
    if (skipHardwareCheck) {
      dataBuffer.put((byte) 1);
    }
    byte[] data = dataBuffer.array();
    byte[] tag = HmacProtocol.calculateTag(data, salt);
    ByteBuffer payloadBuffer = ByteBuffer.allocate(data.length + tag.length);
    payloadBuffer.put(data);
    payloadBuffer.put(tag);
    return new Base32().encodeAsString(payloadBuffer.array());
  }

  private static Properties decryptV02(String activationCode, List<String> systemInfoList)
      throws Exception {
    Properties properties = new Properties();
    properties.load(new StringReader(Bandit.publicDecryptV02(activationCode, systemInfoList)));
    return properties;
  }
}
