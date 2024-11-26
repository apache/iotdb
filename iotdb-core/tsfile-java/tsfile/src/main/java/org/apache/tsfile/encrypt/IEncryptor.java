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
import org.apache.tsfile.file.metadata.enums.EncryptionType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

/** encrypt data according to tsfileconfig. */
public interface IEncryptor {

  static final Logger logger = LoggerFactory.getLogger(IEncryptor.class);

  static IEncryptor getEncryptor(String type, byte[] key) {
    try {
      if (IEncrypt.encryptMap.containsKey(type)) {
        return ((IEncrypt) IEncrypt.encryptMap.get(type).newInstance(key)).getEncryptor();
      }
      Class<?> encryptClass = Class.forName(type);
      java.lang.reflect.Constructor<?> constructor =
          encryptClass.getDeclaredConstructor(byte[].class);
      IEncrypt.encryptMap.put(type, constructor);
      return ((IEncrypt) constructor.newInstance(key)).getEncryptor();
    } catch (ClassNotFoundException e) {
      throw new EncryptException("Get encryptor class failed: " + type, e);
    } catch (NoSuchMethodException e) {
      throw new EncryptException("Get constructor for encryptor failed: " + type, e);
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new EncryptException("New encryptor instance failed: " + type, e);
    }
  }

  static IEncryptor getEncryptor(EncryptParameter encryptParam) {
    String type = encryptParam.getType();
    byte[] key = encryptParam.getKey();
    try {
      if (IEncrypt.encryptMap.containsKey(type)) {
        return ((IEncrypt) IEncrypt.encryptMap.get(type).newInstance(key)).getEncryptor();
      }
      Class<?> encryptClass = Class.forName(type);
      java.lang.reflect.Constructor<?> constructor =
          encryptClass.getDeclaredConstructor(byte[].class);
      IEncrypt.encryptMap.put(type, constructor);
      return ((IEncrypt) constructor.newInstance(key)).getEncryptor();
    } catch (ClassNotFoundException e) {
      throw new EncryptException("Get encryptor class failed: " + type, e);
    } catch (NoSuchMethodException e) {
      throw new EncryptException("Get constructor for encryptor failed: " + type, e);
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new EncryptException("New encryptor instance failed: " + type, e);
    }
  }

  byte[] encrypt(byte[] data);

  byte[] encrypt(byte[] data, int offset, int size);

  EncryptionType getEncryptionType();
}
