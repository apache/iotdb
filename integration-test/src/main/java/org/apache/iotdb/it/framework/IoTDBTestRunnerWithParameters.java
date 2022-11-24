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

import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters;
import org.junit.runners.parameterized.TestWithParameters;
import org.slf4j.Logger;

public class IoTDBTestRunnerWithParameters extends BlockJUnit4ClassRunnerWithParameters {

  private static final Logger logger = IoTDBTestLogger.logger;
  private IoTDBTestListener listener;

  public IoTDBTestRunnerWithParameters(TestWithParameters test) throws InitializationError {
    super(test);
  }

  @Override
  public void run(RunNotifier notifier) {
    listener = new IoTDBTestListener(this.getName());
    notifier.addListener(listener);
    super.run(notifier);
  }

  @Override
  protected synchronized void runChild(final FrameworkMethod method, RunNotifier notifier) {
    Description description = describeChild(method);
    logger.info("Run {}", description.getMethodName());
    long currentTime = System.currentTimeMillis();
    EnvFactory.getEnv().setTestMethodName(description.getMethodName());
    super.runChild(method, notifier);
    double timeCost = (System.currentTimeMillis() - currentTime) / 1000.0;
    String testName = description.getClassName() + "." + description.getMethodName();
    logger.info("Done {}. Cost: {}s", description.getMethodName(), timeCost);
    listener.addTestStat(new IoTDBTestStat(testName, timeCost));
  }
}
