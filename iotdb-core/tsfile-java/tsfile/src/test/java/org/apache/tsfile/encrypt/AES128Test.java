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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class AES128Test {
  private final String key = "mkmkmkmkmkmkmkmk";

  private String randomString(int length) {
    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      builder.append((char) (ThreadLocalRandom.current().nextInt(33, 128)));
    }
    return builder.toString();
  }

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testBytes1() throws IOException {
    String input = randomString(2000000);
    byte[] unencrypted = input.getBytes(StandardCharsets.UTF_8);
    long time = System.currentTimeMillis();
    IEncryptor encryptor = new AES128Encryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] encrypted = encryptor.encrypt(unencrypted);
    System.out.println("encryption time cost:" + (System.currentTimeMillis() - time));
    time = System.currentTimeMillis();
    IDecryptor decryptor = new AES128Decryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] decrypted = decryptor.decrypt(encrypted);
    System.out.println("decryption time cost:" + (System.currentTimeMillis() - time));
    Assert.assertArrayEquals(unencrypted, decrypted);
  }

  @Test
  public void testBytes2() throws IOException {
    String input = randomString(500000);
    byte[] unencrypted = input.getBytes(StandardCharsets.UTF_8);
    long time = System.currentTimeMillis();
    IEncryptor encryptor = new AES128Encryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] encrypted = encryptor.encrypt(unencrypted, 0, unencrypted.length);
    System.out.println("encryption time cost:" + (System.currentTimeMillis() - time));
    time = System.currentTimeMillis();
    IDecryptor decryptor = new AES128Decryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] decrypted = decryptor.decrypt(encrypted, 0, encrypted.length);
    System.out.println("decryption time cost:" + (System.currentTimeMillis() - time));
    Assert.assertArrayEquals(unencrypted, decrypted);
  }

  @Test
  public void testBytes3() throws IOException {
    byte[] unencrypted = new byte[71];
    long time = System.currentTimeMillis();
    IEncryptor encryptor = new AES128Encryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] encrypted = encryptor.encrypt(unencrypted, 0, 35);
    System.out.println("encryption time cost:" + (System.currentTimeMillis() - time));
    time = System.currentTimeMillis();
    IDecryptor decryptor = new AES128Decryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] decrypted = decryptor.decrypt(encrypted);
    System.out.println("decryption time cost:" + (System.currentTimeMillis() - time));
  }
}
