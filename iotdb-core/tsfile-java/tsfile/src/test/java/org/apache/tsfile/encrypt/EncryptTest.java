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
package org.apache.tsfile.encrypt;

import org.apache.tsfile.file.metadata.enums.EncryptionType;
import org.apache.tsfile.utils.PublicBAOS;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class EncryptTest {
  private final String inputString = "AES, a fast encryptor/decryptor.";
  private final String key = "mkmkmkmkmkmkmkmk";

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void NoEncryptorTest() throws IOException {
    IEncryptor encryptor = new NoEncryptor(key.getBytes(StandardCharsets.UTF_8));
    IDecryptor decryptor = new NoDecryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] encrypted = encryptor.encrypt(inputString.getBytes(StandardCharsets.UTF_8));
    byte[] decrypted = decryptor.decrypt(encrypted);

    String result = new String(decrypted, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void AES128Test() throws IOException {
    IEncryptor encryptor = new AES128Encryptor(key.getBytes(StandardCharsets.UTF_8));
    IDecryptor decryptor = new AES128Decryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] encrypted = encryptor.encrypt(inputString.getBytes(StandardCharsets.UTF_8));
    byte[] decrypted = decryptor.decrypt(encrypted);

    String result = new String(decrypted, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void AES128Test1() throws IOException {
    PublicBAOS out = new PublicBAOS();
    out.write(inputString.getBytes(StandardCharsets.UTF_8));
    IEncryptor encryptor = new AES128Encryptor(key.getBytes(StandardCharsets.UTF_8));
    IDecryptor decryptor = new AES128Decryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] encrypted = encryptor.encrypt(out.getBuf(), 0, out.size());
    byte[] decrypted = decryptor.decrypt(encrypted);

    String result = new String(decrypted, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void AES128Test3() throws IOException {
    IEncryptor encryptor =
        IEncryptor.getEncryptor(
            "org.apache.tsfile.encrypt.AES128", key.getBytes(StandardCharsets.UTF_8));
    IDecryptor decryptor =
        IDecryptor.getDecryptor(
            "org.apache.tsfile.encrypt.AES128", key.getBytes(StandardCharsets.UTF_8));
    byte[] encrypted = encryptor.encrypt(inputString.getBytes(StandardCharsets.UTF_8));
    byte[] decrypted = decryptor.decrypt(encrypted);

    String result = new String(decrypted, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void GetEncryptorTest() {
    IEncryptor encryptor =
        IEncryptor.getEncryptor(
            "org.apache.tsfile.encrypt.AES128", key.getBytes(StandardCharsets.UTF_8));
    assertEquals(encryptor.getEncryptionType(), EncryptionType.AES128);
    IEncryptor encryptor2 =
        IEncryptor.getEncryptor(
            "org.apache.tsfile.encrypt.UNENCRYPTED", key.getBytes(StandardCharsets.UTF_8));
    assertEquals(encryptor2.getEncryptionType(), EncryptionType.UNENCRYPTED);
  }

  @Test
  public void GetDecryptorTest() {
    IEncryptor encryptor =
        IEncryptor.getEncryptor(
            "org.apache.tsfile.encrypt.AES128", key.getBytes(StandardCharsets.UTF_8));
    assertEquals(encryptor.getEncryptionType(), EncryptionType.AES128);
    IEncryptor encryptor2 =
        IEncryptor.getEncryptor(
            "org.apache.tsfile.encrypt.UNENCRYPTED", key.getBytes(StandardCharsets.UTF_8));
    assertEquals(encryptor2.getEncryptionType(), EncryptionType.UNENCRYPTED);
  }

  @Test
  public void HexStringTransverse() {
    byte[] SboxTable = {
      (byte) 0xd6,
      (byte) 0x90,
      (byte) 0xe9,
      (byte) 0xfe,
      (byte) 0xcc,
      (byte) 0xe1,
      0x3d,
      (byte) 0xb7,
      0x16,
      (byte) 0xb6,
      0x14,
      (byte) 0xc2,
      0x28,
      (byte) 0xfb,
      0x2c,
      0x05,
      0x2b,
      0x67,
      (byte) 0x9a,
      0x76,
      0x2a,
      (byte) 0xbe,
      0x04,
      (byte) 0xc3,
      (byte) 0xaa,
      0x44,
      0x13,
      0x26,
      0x49,
      (byte) 0x86,
      0x06,
      (byte) 0x99,
      (byte) 0x9c,
      0x42,
      0x50,
      (byte) 0xf4,
      (byte) 0x91,
      (byte) 0xef,
      (byte) 0x98,
      0x7a,
      0x33,
      0x54,
      0x0b,
      0x43,
      (byte) 0xed,
      (byte) 0xcf,
      (byte) 0xac,
      0x62,
      (byte) 0xe4,
      (byte) 0xb3,
      0x1c,
      (byte) 0xa9,
      (byte) 0xc9,
      0x08,
      (byte) 0xe8,
      (byte) 0x95,
      (byte) 0x80,
      (byte) 0xdf,
      (byte) 0x94,
      (byte) 0xfa,
      0x75,
      (byte) 0x8f,
      0x3f,
      (byte) 0xa6,
      0x47,
      0x07,
      (byte) 0xa7,
      (byte) 0xfc,
      (byte) 0xf3,
      0x73,
      0x17,
      (byte) 0xba,
      (byte) 0x83,
      0x59,
      0x3c,
      0x19,
      (byte) 0xe6,
      (byte) 0x85,
      0x4f,
      (byte) 0xa8,
      0x68,
      0x6b,
      (byte) 0x81,
      (byte) 0xb2,
      0x71,
      0x64,
      (byte) 0xda,
      (byte) 0x8b,
      (byte) 0xf8,
      (byte) 0xeb,
      0x0f,
      0x4b,
      0x70,
      0x56,
      (byte) 0x9d,
      0x35,
      0x1e,
      0x24,
      0x0e,
      0x5e,
      0x63,
      0x58,
      (byte) 0xd1,
      (byte) 0xa2,
      0x25,
      0x22,
      0x7c,
      0x3b,
      0x01,
      0x21,
      0x78,
      (byte) 0x87,
      (byte) 0xd4,
      0x00,
      0x46,
      0x57,
      (byte) 0x9f,
      (byte) 0xd3,
      0x27,
      0x52,
      0x4c,
      0x36,
      0x02,
      (byte) 0xe7,
      (byte) 0xa0,
      (byte) 0xc4,
      (byte) 0xc8,
      (byte) 0x9e,
      (byte) 0xea,
      (byte) 0xbf,
      (byte) 0x8a,
      (byte) 0xd2,
      0x40,
      (byte) 0xc7,
      0x38,
      (byte) 0xb5,
      (byte) 0xa3,
      (byte) 0xf7,
      (byte) 0xf2,
      (byte) 0xce,
      (byte) 0xf9,
      0x61,
      0x15,
      (byte) 0xa1,
      (byte) 0xe0,
      (byte) 0xae,
      0x5d,
      (byte) 0xa4,
      (byte) 0x9b,
      0x34,
      0x1a,
      0x55,
      (byte) 0xad,
      (byte) 0x93,
      0x32,
      0x30,
      (byte) 0xf5,
      (byte) 0x8c,
      (byte) 0xb1,
      (byte) 0xe3,
      0x1d,
      (byte) 0xf6,
      (byte) 0xe2,
      0x2e,
      (byte) 0x82,
      0x66,
      (byte) 0xca,
      0x60,
      (byte) 0xc0,
      0x29,
      0x23,
      (byte) 0xab,
      0x0d,
      0x53,
      0x4e,
      0x6f,
      (byte) 0xd5,
      (byte) 0xdb,
      0x37,
      0x45,
      (byte) 0xde,
      (byte) 0xfd,
      (byte) 0x8e,
      0x2f,
      0x03,
      (byte) 0xff,
      0x6a,
      0x72,
      0x6d,
      0x6c,
      0x5b,
      0x51,
      (byte) 0x8d,
      0x1b,
      (byte) 0xaf,
      (byte) 0x92,
      (byte) 0xbb,
      (byte) 0xdd,
      (byte) 0xbc,
      0x7f,
      0x11,
      (byte) 0xd9,
      0x5c,
      0x41,
      0x1f,
      0x10,
      0x5a,
      (byte) 0xd8,
      0x0a,
      (byte) 0xc1,
      0x31,
      (byte) 0x88,
      (byte) 0xa5,
      (byte) 0xcd,
      0x7b,
      (byte) 0xbd,
      0x2d,
      0x74,
      (byte) 0xd0,
      0x12,
      (byte) 0xb8,
      (byte) 0xe5,
      (byte) 0xb4,
      (byte) 0xb0,
      (byte) 0x89,
      0x69,
      (byte) 0x97,
      0x4a,
      0x0c,
      (byte) 0x96,
      0x77,
      0x7e,
      0x65,
      (byte) 0xb9,
      (byte) 0xf1,
      0x09,
      (byte) 0xc5,
      0x6e,
      (byte) 0xc6,
      (byte) 0x84,
      0x18,
      (byte) 0xf0,
      0x7d,
      (byte) 0xec,
      0x3a,
      (byte) 0xdc,
      0x4d,
      0x20,
      0x79,
      (byte) 0xee,
      0x5f,
      0x3e,
      (byte) 0xd7,
      (byte) 0xcb,
      0x39,
      0x48
    };
    String ttt = EncryptUtils.byteArrayToHexString(SboxTable);
    byte[] bytes = EncryptUtils.hexStringToByteArray(ttt);
    assertArrayEquals(SboxTable, bytes);
  }
}
