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

import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.exception.encrypt.EncryptKeyLengthNotMatchException;
import org.apache.tsfile.file.metadata.enums.EncryptionType;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class AES128Decryptor implements IDecryptor {
  private final Cipher AES;

  private final SecretKeySpec secretKeySpec;

  private final IvParameterSpec ivParameterSpec;

  AES128Decryptor(byte[] key) {
    if (key.length != 16) {
      throw new EncryptKeyLengthNotMatchException(16, key.length);
    }
    secretKeySpec = new SecretKeySpec(key, "AES");
    // Create IV parameter
    ivParameterSpec = new IvParameterSpec(key);
    try {
      // Create Cipher instance and initialize it for encryption in CTR mode without padding
      this.AES = Cipher.getInstance("AES/CTR/NoPadding");
      AES.init(Cipher.DECRYPT_MODE, secretKeySpec, ivParameterSpec);
    } catch (InvalidAlgorithmParameterException
        | NoSuchPaddingException
        | NoSuchAlgorithmException
        | InvalidKeyException e) {
      throw new EncryptException("AES128Decryptor init failed ", e);
    }
  }

  @Override
  public byte[] decrypt(byte[] data) {
    try {
      return AES.doFinal(data);
    } catch (IllegalBlockSizeException | BadPaddingException e) {
      throw new EncryptException("AES128Decryptor decrypt failed ", e);
    }
  }

  @Override
  public byte[] decrypt(byte[] data, int offset, int size) {
    try {
      return AES.doFinal(data, offset, size);
    } catch (IllegalBlockSizeException | BadPaddingException e) {
      throw new EncryptException("AES128Decryptor decrypt failed ", e);
    }
  }

  @Override
  public EncryptionType getEncryptionType() {
    return EncryptionType.AES128;
  }
}
