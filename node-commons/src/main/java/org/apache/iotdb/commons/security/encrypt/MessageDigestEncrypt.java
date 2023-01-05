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

package org.apache.iotdb.commons.security.encrypt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MessageDigestEncrypt implements AsymmetricEncrypt {
  private static final Logger logger = LoggerFactory.getLogger(MessageDigestEncrypt.class);

  private static final String ENCRYPT_ALGORITHM = "MD5";
  private static final String STRING_ENCODING = "utf-8";

  @Override
  public void init(String providerParameters) {}

  @Override
  public String encrypt(String originPassword) {
    try {
      MessageDigest messageDigest = MessageDigest.getInstance(ENCRYPT_ALGORITHM);
      messageDigest.update(originPassword.getBytes(STRING_ENCODING));
      return new String(messageDigest.digest(), STRING_ENCODING);
    } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
      logger.error("meet error while encrypting password.", e);
      return originPassword;
    }
  }

  @Override
  public boolean validate(String originPassword, String encryptPassword) {
    if (originPassword == null) {
      return false;
    }
    return encrypt(originPassword).equals(encryptPassword);
  }
}
