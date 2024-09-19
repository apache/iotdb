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

package org.apache.iotdb.it.framework;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.EnvType;
import org.apache.iotdb.it.env.MultiEnvFactory;

import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;

import java.util.TimeZone;

public class IoTDBTestRunner extends BlockJUnit4ClassRunner {

  private static final Logger logger = IoTDBTestLogger.logger;
  private IoTDBTestListener listener;

  public IoTDBTestRunner(final Class<?> testClass) throws InitializationError {
    super(testClass);
  }

  @Override
  public void run(final RunNotifier notifier) {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC+08:00"));
    listener = new IoTDBTestListener(this.getName());
    notifier.addListener(listener);
    super.run(notifier);
  }

  @Override
  protected void runChild(final FrameworkMethod method, final RunNotifier notifier) {
    final Description description = describeChild(method);
    logger.info("Run {}", description.getMethodName());
    final long currentTime = System.currentTimeMillis();
    if (EnvType.getSystemEnvType() != EnvType.MultiCluster) {
      EnvFactory.getEnv().setTestMethodName(description.getMethodName());
    }
    MultiEnvFactory.setTestMethodName(description.getMethodName());
    super.runChild(method, notifier);
    final double timeCost = (System.currentTimeMillis() - currentTime) / 1000.0;
    final String testName = description.getClassName() + "." + description.getMethodName();
    logger.info("Done {}. Cost: {}s", description.getMethodName(), timeCost);
    listener.addTestStat(new IoTDBTestStat(testName, timeCost));
  }
}
