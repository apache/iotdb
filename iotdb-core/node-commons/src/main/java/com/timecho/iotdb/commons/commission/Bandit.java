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

import org.apache.iotdb.commons.exception.LicenseException;

import com.timecho.iotdb.commons.commission.complete.HmacProtocol;
import com.timecho.iotdb.commons.external.codec.binary.Base32;
import com.timecho.iotdb.commons.external.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

public class Bandit {
  private static final Logger LOG = LoggerFactory.getLogger(Bandit.class);

  private static final String RSA_PUBLIC_KEY =
      "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCAWzhK/X+CtDhMQyUzVEQH/euInXQxbC4vXPi/HNvhBqg4p4fGN600OFWpuB3ToGgL2wOP5+JVaOUqUh7GRSEUzzPt+GGOLGujuzsT5auZ/TgJmsWUTsL555bYYp5euoPBy/KdbkjMKR8vGTITG75ngj2JhN+1etYkUlgUfuhoOQIDAQAB";

  private static final String RSA_ALGORITHM_NO_PADDING = "RSA";
  public static final String RSA_ALGORITHM = "RSA/ECB/PKCS1Padding";

  private static final int MAX_ENCRYPT_BLOCK = 117;

  private static final int MAX_DECRYPT_BLOCK = 128;

  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  public static final String CIPHER_ENCRYPT = "encrypt";
  public static final String CIPHER_DECRYPT = "decrypt";

  private static final String ILLEGAL_LICENSE = "illegal license";

  private Bandit() {
    throw new IllegalStateException("Utility class");
  }

  private static PublicKey getPublicKey() throws NoSuchAlgorithmException, InvalidKeySpecException {
    X509EncodedKeySpec x509EncodedKeySpec =
        new X509EncodedKeySpec(Base64.decodeBase64(RSA_PUBLIC_KEY));
    KeyFactory keyFactory = KeyFactory.getInstance(RSA_ALGORITHM_NO_PADDING);
    return keyFactory.generatePublic(x509EncodedKeySpec);
  }

  private static String section(
      String type,
      String src,
      Cipher cipher,
      Function<byte[], String> encoder,
      Function<String, byte[]> decoder)
      throws LicenseException {
    try {
      if (CIPHER_ENCRYPT.equals(type)) {
        byte[] bytes = src.getBytes(DEFAULT_CHARSET);
        int inputLen = bytes.length;
        int offSet = 0;
        byte[] cache;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int i = 0;
        while (inputLen - offSet > 0) {
          if (inputLen - offSet > MAX_ENCRYPT_BLOCK) {
            cache = cipher.doFinal(bytes, offSet, MAX_ENCRYPT_BLOCK);
          } else {
            cache = cipher.doFinal(bytes, offSet, inputLen - offSet);
          }
          out.write(cache, 0, cache.length);
          i++;
          offSet = i * MAX_ENCRYPT_BLOCK;
        }
        byte[] encryptedData = out.toByteArray();
        out.close();

        return encoder.apply(encryptedData);
      } else if (CIPHER_DECRYPT.equals(type)) {
        byte[] bytes = decoder.apply(src);
        int inputLen = bytes.length;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int offSet = 0;
        byte[] cache;
        int i = 0;
        while (inputLen - offSet > 0) {
          if (inputLen - offSet > MAX_DECRYPT_BLOCK) {
            cache = cipher.doFinal(bytes, offSet, MAX_DECRYPT_BLOCK);
          } else {
            cache = cipher.doFinal(bytes, offSet, inputLen - offSet);
          }
          out.write(cache, 0, cache.length);
          i++;
          offSet = i * MAX_DECRYPT_BLOCK;
        }
        out.close();

        return out.toString();
      }
    } catch (Exception e) {
      throw new LicenseException(ILLEGAL_LICENSE);
    }
    return "";
  }

  private static String sectionV00(String type, String src, Cipher cipher) throws LicenseException {
    return section(type, src, cipher, Base64::encodeBase64String, Base64::decodeBase64);
  }

  private static String sectionV01(String type, String src, Cipher cipher) throws LicenseException {
    Base32 base32 = new Base32();
    return section(type, src, cipher, base32::encodeAsString, base32::decode);
  }

  public static String publicEncrypt(String src) throws LicenseException {
    try {
      Cipher cipher = Cipher.getInstance(RSA_ALGORITHM);
      cipher.init(Cipher.ENCRYPT_MODE, getPublicKey());
      return sectionV01(CIPHER_ENCRYPT, src, cipher);
    } catch (Exception e) {
      throw new LicenseException(ILLEGAL_LICENSE);
    }
  }

  public static String publicDecryptV00(String src) throws LicenseException {
    try {
      Cipher cipher = Cipher.getInstance(RSA_ALGORITHM);
      cipher.init(Cipher.DECRYPT_MODE, getPublicKey());
      return sectionV00(CIPHER_DECRYPT, src, cipher);
    } catch (Exception e) {
      throw new LicenseException(ILLEGAL_LICENSE);
    }
  }

  public static String publicDecryptV01(String src) throws LicenseException {
    try {
      Cipher cipher = Cipher.getInstance(RSA_ALGORITHM);
      cipher.init(Cipher.DECRYPT_MODE, getPublicKey());
      return sectionV01(CIPHER_DECRYPT, src, cipher);
    } catch (Exception e) {
      throw new LicenseException(ILLEGAL_LICENSE);
    }
  }

  private static String publicDecryptCommon(
      String src, java.util.function.Function<byte[], byte[]> tagCalculator, Optional<String> s9)
      throws LicenseException {
    Base32 base32 = new Base32();
    byte[] payload = base32.decode(src);

    /* Verify the length (the signature is fixed at HmacProtocol.SIGN_LENGTH bytes, so the payload must be at least more than HmacProtocol.SIGN_LENGTH bytes) */
    if (payload.length <= HmacProtocol.SIGN_LENGTH) {
      throw new SecurityException("Invalid activation code format");
    }

    ByteBuffer buf = ByteBuffer.wrap(payload);
    int dataCapacity = buf.capacity() - HmacProtocol.SIGN_LENGTH;
    byte[] data = new byte[dataCapacity];
    buf.get(data);
    byte identifier = data[0];

    byte[] sigBytes = new byte[HmacProtocol.SIGN_LENGTH];
    buf.get(sigBytes);

    try {
      byte[] expectedTag = tagCalculator.apply(data);

      if (!Arrays.equals(sigBytes, expectedTag)) {
        throw new SecurityException("Signature verification failed");
      }

      // Parse fields
      Properties properties = unpack(data, identifier);

      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

      StringBuilder builder = new StringBuilder();
      for (String key : properties.stringPropertyNames()) {
        String value = properties.getProperty(key);

        if (key.equals(Lottery.LICENSE_EXPIRE_TIMESTAMP_NAME)) {
          if (value != null && !value.isEmpty()) {
            LocalDate localDate = LocalDate.parse(value, formatter);
            long timestamp =
                localDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
            builder.append(key).append("=").append(timestamp).append("\n");
          }
        } else {
          builder.append(key).append("=").append(properties.getProperty(key)).append("\n");
        }
      }
      if (s9.isPresent()) {
        builder.append(Lottery.SYSTEM_INFO_HASH).append("=").append(s9.get()).append("\n");
      } else {
        builder
            .append(Lottery.RELEASE_TYPE_NAME)
            .append("=")
            .append(Lottery.RELEASE_TYPE_TRIAL)
            .append("\n");
      }
      return builder.toString();
    } catch (Exception e) {
      LOG.error("License decryption error", e);
      throw new LicenseException(ILLEGAL_LICENSE);
    }
  }

  /** Decrypt V02 version activation code */
  public static String publicDecryptV02(String src, List<String> systemInfoList)
      throws LicenseException {
    String localS9 = String.join(",", systemInfoList);
    return publicDecryptCommon(
        src,
        data -> {
          try {
            return HmacProtocol.calculateTag(data, localS9);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        Optional.of(localS9));
  }

  /** Parse V03 version trial activation code */
  public static String publicDecryptV03(String src) throws LicenseException {
    return publicDecryptCommon(
        src,
        data -> {
          try {
            return HmacProtocol.calculateTag(data);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        Optional.empty());
  }

  public static Properties unpack(byte[] data, byte identifier) {
    ByteBuffer buffer = ByteBuffer.wrap(data);
    buffer.get();
    int l2 = buffer.getInt();

    Properties p = new Properties();
    p.setProperty(Lottery.LICENSE_EXPIRE_TIMESTAMP_NAME, String.valueOf(l2));

    for (int i = 0; i < Lottery.CAPACITY_CONFIG.size(); i++) {
      if ((identifier & (int) Math.pow(2, i)) == Math.pow(2, i)) {
        switch (i) {
          case 0:
            byte dn1 = buffer.get();
            p.setProperty(Lottery.DATANODE_NUM_LIMIT_NAME, String.valueOf(dn1 & 0xFF));
            break;
          case 1:
            int dn2 = buffer.getInt();
            p.setProperty(Lottery.DATANODE_CPU_CORE_NUM_LIMIT_NAME, String.valueOf(dn2));
            break;
          case 2:
            long dn4 = buffer.getLong();
            p.setProperty(Lottery.SENSOR_NUM_LIMIT_NAME, String.valueOf(dn4));
            break;
          default:
            byte ml1 = buffer.get();
            p.setProperty(Lottery.AINODE_NUM_LIMIT_NAME, String.valueOf(ml1 & 0xFF));
            break;
        }
      }
    }

    return p;
  }
}
