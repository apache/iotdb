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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.exception.encrypt.EncryptException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.MessageDigest;
import java.util.Arrays;

public class EncryptUtils {

  private static final Logger logger = LoggerFactory.getLogger(EncryptUtils.class);

  private static final String defaultKey = "abcdefghijklmnop";

  public static String normalKeyStr = getNormalKeyStr();

  public static EncryptParameter encryptParam = getEncryptParameter();

  public static String getEncryptKeyFromPath(String path) {
    if (path == null) {
      logger.error("encrypt key path is null, use the default key");
      return defaultKey;
    }
    if (path.isEmpty()) {
      logger.error("encrypt key path is empty, use the default key");
      return defaultKey;
    }
    try (BufferedReader br = new BufferedReader(new FileReader(path))) {
      StringBuilder sb = new StringBuilder();
      String line;
      boolean first = true;
      while ((line = br.readLine()) != null) {
        if (first) {
          sb.append(line);
          first = false;
        } else {
          sb.append("\n").append(line);
        }
      }
      return sb.toString();
    } catch (IOException e) {
      throw new EncryptException("Read main encrypt key error", e);
    }
  }

  public static byte[] hexStringToByteArray(String hexString) {
    int len = hexString.length();
    byte[] byteArray = new byte[len / 2];

    for (int i = 0; i < len; i += 2) {
      byteArray[i / 2] =
          (byte)
              ((Character.digit(hexString.charAt(i), 16) << 4)
                  + Character.digit(hexString.charAt(i + 1), 16));
    }

    return byteArray;
  }

  public static String byteArrayToHexString(byte[] bytes) {
    StringBuilder sb = new StringBuilder();

    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }

    return sb.toString();
  }

  public static String getNormalKeyStr() {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update("IoTDB is the best".getBytes());
      md.update(TSFileDescriptor.getInstance().getConfig().getEncryptKey().getBytes());
      byte[] data_key = Arrays.copyOfRange(md.digest(), 0, 16);
      data_key =
          IEncryptor.getEncryptor(
                  TSFileDescriptor.getInstance().getConfig().getEncryptType(),
                  TSFileDescriptor.getInstance().getConfig().getEncryptKey().getBytes())
              .encrypt(data_key);

      StringBuilder valueStr = new StringBuilder();

      for (byte b : data_key) {
        valueStr.append(b).append(",");
      }

      valueStr.deleteCharAt(valueStr.length() - 1);
      String str = valueStr.toString();

      return str;
    } catch (Exception e) {
      throw new EncryptException(
          "SHA-256 function not found while using SHA-256 to generate data key");
    }
  }

  public static String getNormalKeyStr(TSFileConfig conf) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update("IoTDB is the best".getBytes());
      md.update(conf.getEncryptKey().getBytes());
      byte[] data_key = Arrays.copyOfRange(md.digest(), 0, 16);
      data_key =
          IEncryptor.getEncryptor(conf.getEncryptType(), conf.getEncryptKey().getBytes())
              .encrypt(data_key);

      StringBuilder valueStr = new StringBuilder();

      for (byte b : data_key) {
        valueStr.append(b).append(",");
      }

      valueStr.deleteCharAt(valueStr.length() - 1);
      String str = valueStr.toString();

      return str;
    } catch (Exception e) {
      throw new EncryptException(
          "SHA-256 function not found while using SHA-256 to generate data key", e);
    }
  }

  public static EncryptParameter getEncryptParameter() {
    return getEncryptParameter(TSFileDescriptor.getInstance().getConfig());
  }

  public static EncryptParameter getEncryptParameter(TSFileConfig conf) {
    String encryptType;
    byte[] dataEncryptKey;
    if (conf.getEncryptFlag()) {
      encryptType = conf.getEncryptType();
      try {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update("IoTDB is the best".getBytes());
        md.update(conf.getEncryptKey().getBytes());
        dataEncryptKey = Arrays.copyOfRange(md.digest(), 0, 16);
      } catch (Exception e) {
        throw new EncryptException(
            "SHA-256 function not found while using SHA-256 to generate data key", e);
      }
    } else {
      encryptType = "org.apache.tsfile.encrypt.UNENCRYPTED";
      dataEncryptKey = null;
    }
    return new EncryptParameter(encryptType, dataEncryptKey);
  }

  public static IEncrypt getEncrypt() {
    return getEncrypt(TSFileDescriptor.getInstance().getConfig());
  }

  public static IEncrypt getEncrypt(String encryptType, byte[] dataEncryptKey) {
    try {
      if (IEncrypt.encryptMap.containsKey(encryptType)) {
        return ((IEncrypt) IEncrypt.encryptMap.get(encryptType).newInstance(dataEncryptKey));
      }
      Class<?> encryptTypeClass = Class.forName(encryptType);
      java.lang.reflect.Constructor<?> constructor =
          encryptTypeClass.getDeclaredConstructor(byte[].class);
      IEncrypt.encryptMap.put(encryptType, constructor);
      return ((IEncrypt) constructor.newInstance(dataEncryptKey));
    } catch (ClassNotFoundException e) {
      throw new EncryptException("Get encryptor class failed: " + encryptType, e);
    } catch (NoSuchMethodException e) {
      throw new EncryptException("Get constructor for encryptor failed: " + encryptType, e);
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new EncryptException("New encryptor instance failed: " + encryptType, e);
    }
  }

  public static IEncrypt getEncrypt(TSFileConfig conf) {
    String encryptType;
    byte[] dataEncryptKey;
    if (conf.getEncryptFlag()) {
      encryptType = conf.getEncryptType();
      try {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update("IoTDB is the best".getBytes());
        md.update(conf.getEncryptKey().getBytes());
        dataEncryptKey = Arrays.copyOfRange(md.digest(), 0, 16);
      } catch (Exception e) {
        throw new EncryptException(
            "SHA-256 function not found while using SHA-256 to generate data key", e);
      }
    } else {
      encryptType = "org.apache.tsfile.encrypt.UNENCRYPTED";
      dataEncryptKey = null;
    }
    try {
      Class<?> encryptTypeClass = Class.forName(encryptType);
      java.lang.reflect.Constructor<?> constructor =
          encryptTypeClass.getDeclaredConstructor(byte[].class);
      return ((IEncrypt) constructor.newInstance(dataEncryptKey));
    } catch (ClassNotFoundException e) {
      throw new EncryptException("Get encryptor class failed: " + encryptType, e);
    } catch (NoSuchMethodException e) {
      throw new EncryptException("Get constructor for encryptor failed: " + encryptType, e);
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new EncryptException("New encryptor instance failed: " + encryptType, e);
    }
  }

  public static byte[] getSecondKeyFromStr(String str) {
    String[] strArray = str.split(",");
    byte[] key = new byte[strArray.length];
    for (int i = 0; i < strArray.length; i++) {
      key[i] = Byte.parseByte(strArray[i]);
    }
    return key;
  }
}
