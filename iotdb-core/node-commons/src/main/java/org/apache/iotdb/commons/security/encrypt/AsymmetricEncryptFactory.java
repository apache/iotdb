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

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicReference;

public class AsymmetricEncryptFactory {

  private static final AtomicReference<AsymmetricEncrypt> asymmetricEncrypt =
      new AtomicReference<>();

  private AsymmetricEncryptFactory() {
    // Empty constructor
  }

  /**
   * load encrypt provider class for encrypt or decrypt password.
   *
   * @param providerClassName encrypt class name
   * @param providerParameter provider parameter
   * @return AsymmetricEncrypt instance
   */
  public static AsymmetricEncrypt getEncryptProvider(
      String providerClassName, String providerParameter) {
    if (asymmetricEncrypt.get() == null) {
      synchronized (AsymmetricEncrypt.class) {
        if (asymmetricEncrypt.get() == null) {

          try {
            Class<?> providerClass =
                getClassLoaderForClass(AsymmetricEncrypt.class).loadClass(providerClassName);
            asymmetricEncrypt.set(
                (AsymmetricEncrypt) providerClass.getDeclaredConstructor().newInstance());
            asymmetricEncrypt.get().init(providerParameter);
          } catch (ClassNotFoundException
              | NoSuchMethodException
              | InstantiationException
              | IllegalAccessException
              | InvocationTargetException e) {
            throw new EncryptDecryptException(e);
          }
        }
      }
    }
    return asymmetricEncrypt.get();
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
