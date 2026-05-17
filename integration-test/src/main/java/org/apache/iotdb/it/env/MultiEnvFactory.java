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

package org.apache.iotdb.it.env;

import org.apache.iotdb.it.env.cluster.env.MultiClusterEnv;
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.Config;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class MultiEnvFactory {
  private static final List<BaseEnv> envList = new ArrayList<>();
  private static final Logger logger = IoTDBTestLogger.logger;
  private static String currentMethodName;
  private static String currentTestClassName;

  private MultiEnvFactory() {
    // Empty constructor
  }

  static {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                MultiEnvFactory::cleanupEnvs,
                MultiEnvFactory.class.getSimpleName() + "-ShutdownHook"));
  }

  public static void setTestMethodName(final String testMethodName) {
    currentMethodName = testMethodName;
    envList.forEach(baseEnv -> baseEnv.setTestMethodName(testMethodName));
  }

  /** Get an environment with the specific index. */
  public static BaseEnv getEnv(final int index) throws IndexOutOfBoundsException {
    return envList.get(index);
  }

  /** Create several environments according to the specific number. */
  public static synchronized void createEnv(final int num) {
    final String requestedTestClassName = detectCurrentTestClassName();
    if (requestedTestClassName.equals(currentTestClassName) && envList.size() == num) {
      return;
    }

    cleanupEnvs();

    currentTestClassName = requestedTestClassName;
    final long startTime = System.currentTimeMillis();
    for (int i = 0; i < num; ++i) {
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
        envList.add(new MultiClusterEnv(startTime, i, currentMethodName));
      } catch (final ClassNotFoundException e) {
        logger.error("Create env error", e);
        System.exit(-1);
      }
    }
  }

  public static synchronized void cleanupEnvs() {
    for (final BaseEnv baseEnv : envList) {
      try {
        baseEnv.cleanClusterEnvironment();
      } catch (final Exception e) {
        logger.warn("Cleanup env error", e);
      }
    }
    envList.clear();
    currentTestClassName = null;
  }

  private static String detectCurrentTestClassName() {
    final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    for (final StackTraceElement stackTraceElement : stack) {
      final String className = stackTraceElement.getClassName();
      if (className.endsWith("IT")) {
        final String simpleClassName = className.substring(className.lastIndexOf('.') + 1);
        if (!simpleClassName.startsWith("Abstract")) {
          return className;
        }
      }
    }
    return "UNKNOWN-IT";
  }
}
