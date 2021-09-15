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
package org.apache.iotdb.cluster.partition;

import org.apache.iotdb.db.metadata.MManager;

import org.mockito.internal.util.reflection.Whitebox;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class MManagerWhiteBox {

  public static MManager newMManager(String logFilePath) {
    Constructor<MManager> constructor = getMManagerConstructor();
    constructor.setAccessible(true);
    try {
      MManager manager = constructor.newInstance();
      new File(logFilePath).getParentFile().mkdirs();
      Whitebox.setInternalState(manager, "logFilePath", logFilePath);
      manager.initForMultiMManagerTest();
      return manager;
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      e.printStackTrace();
    }
    return null;
  }

  private static Constructor<MManager> getMManagerConstructor() {
    try {
      return MManager.class.getDeclaredConstructor();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }
    return null;
  }
}
