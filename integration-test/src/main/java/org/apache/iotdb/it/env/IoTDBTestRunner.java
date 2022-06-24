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

import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBTestRunner extends BlockJUnit4ClassRunner {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBTestRunner.class);

  public IoTDBTestRunner(Class<?> testClass) throws InitializationError {
    super(testClass);
  }

  @Override
  protected void runChild(final FrameworkMethod method, RunNotifier notifier) {
    Description description = describeChild(method);
    logger.info("Run {}", description.getMethodName());
    long currentTime = System.currentTimeMillis();
    EnvFactory.getEnv().setTestMethodName(description.getMethodName());
    super.runChild(method, notifier);
    logger.info(
        "Done {}. Cost: {}s",
        description.getMethodName(),
        (System.currentTimeMillis() - currentTime) / 1000.0);
  }
}
