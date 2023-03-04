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

package org.apache.iotdb.db.security.encrypt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

public class AsymmetricEncryptFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(AsymmetricEncryptFactory.class);

  private static volatile AsymmetricEncrypt asymmetricEncrypt;

  /**
   * load encrypt provider class for encrypt or decrypt password
   *
   * @param providerClassName encrypt class name
   * @param providerParameter provider parameter
   * @return AsymmetricEncrypt instance
   */
  public static AsymmetricEncrypt getEncryptProvider(
      String providerClassName, String providerParameter) {
    if (asymmetricEncrypt == null) {
      synchronized (AsymmetricEncrypt.class) {
        if (asymmetricEncrypt == null) {

          try {
            Class providerClass =
                getClassLoaderForClass(AsymmetricEncrypt.class).loadClass(providerClassName);
            asymmetricEncrypt =
                (AsymmetricEncrypt) providerClass.getDeclaredConstructor().newInstance();
            asymmetricEncrypt.init(providerParameter);
          } catch (ClassNotFoundException
              | NoSuchMethodException
              | InstantiationException
              | IllegalAccessException
              | InvocationTargetException e) {
            LOGGER.error("Failed to load encryption class", e);
            throw new EncryptDecryptException(e);
          }
        }
      }
    }
    return asymmetricEncrypt;
  }

  private static ClassLoader getClassLoaderForClass(Class<?> c) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = c.getClassLoader();
    }
    if (cl == null) {
      cl = ClassLoader.getSystemClassLoader();
    }
    if (cl == null) {
      throw new EncryptDecryptException("A ClassLoader to load the class could not be determined.");
    }
    return cl;
  }
}
