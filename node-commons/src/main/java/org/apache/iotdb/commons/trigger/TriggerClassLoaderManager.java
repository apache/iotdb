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

package org.apache.iotdb.commons.trigger;

import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Note: it is NOT thread safe. */
public class TriggerClassLoaderManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerClassLoaderManager.class);

  private final String libRoot;

  private final Map<String, Pair<TriggerClassLoader, Integer>> classNameToClassLoaderUsagePairMap;

  private TriggerClassLoaderManager(String triggerLibRoot) {
    this.libRoot = triggerLibRoot;
    classNameToClassLoaderUsagePairMap = new HashMap<>();
  }

  public TriggerClassLoader register(String className) throws TriggerManagementException {
    Pair<TriggerClassLoader, Integer> classLoaderUsagePair =
        classNameToClassLoaderUsagePairMap.get(className);
    if (classLoaderUsagePair == null) {
      try {
        TriggerClassLoader classLoader = new TriggerClassLoader(libRoot);
        classLoaderUsagePair = new Pair<>(classLoader, 0);
        classNameToClassLoaderUsagePairMap.put(className, classLoaderUsagePair);
        LOGGER.info(
            "A new trigger classloader was constructed for managing trigger class {}.", className);
      } catch (IOException e) {
        throw new TriggerManagementException(
            String.format(
                "Failed to construct a new trigger classloader for managing trigger class %s.",
                className),
            e);
      }
    }
    classLoaderUsagePair.right++;
    return classLoaderUsagePair.left;
  }

  public void deregister(String className) {
    Pair<TriggerClassLoader, Integer> classLoaderUsagePair =
        classNameToClassLoaderUsagePairMap.get(className);
    classLoaderUsagePair.right--;
    if (classLoaderUsagePair.right == 0) {
      try {
        classLoaderUsagePair.left.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close a trigger classloader ({}).", className);
      } finally {
        classNameToClassLoaderUsagePairMap.remove(className);
        LOGGER.info("A trigger classloader ({}) was removed.", className);
      }
    }
  }

  private static TriggerClassLoaderManager INSTANCE = null;

  public static TriggerClassLoaderManager setUpAndGetInstance(String triggerLibRoot) {
    if (INSTANCE == null) {
      INSTANCE = new TriggerClassLoaderManager(triggerLibRoot);
    }
    return INSTANCE;
  }
}
