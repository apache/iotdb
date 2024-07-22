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

package org.apache.iotdb.subscription.it;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;

public class SkipOnSetUpFailure implements TestRule {

  private final String setUpMethodName;

  /**
   * @param setUpMethodName Should be exactly the same as the method name decorated with @Before.
   */
  public SkipOnSetUpFailure(@NonNull final String setUpMethodName) {
    this.setUpMethodName = setUpMethodName;
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          base.evaluate();
        } catch (final Throwable e) {
          // Trace back the exception stack to determine whether the exception was thrown during the
          // setUp phase.
          for (final StackTraceElement stackTraceElement : e.getStackTrace()) {
            if (setUpMethodName.equals(stackTraceElement.getMethodName())
                && description.getClassName().equals(stackTraceElement.getClassName())
                && isMethodAnnotationWithBefore(stackTraceElement.getMethodName())) {
              e.printStackTrace();
              // Skip this test.
              throw new AssumptionViolatedException(
                  String.format(
                      "Skipping test due to setup failure for %s#%s",
                      description.getClassName(), description.getMethodName()));
            }
          }

          // Re-throw the exception (which means the test has failed).
          throw e;

          // Regardless of the circumstances, the method decorated with @After will always be
          // executed.
        }
      }

      private boolean isMethodAnnotationWithBefore(final String methodName) {
        try {
          final Method method = description.getTestClass().getDeclaredMethod(methodName);
          return method.isAnnotationPresent(org.junit.Before.class);
        } catch (final Throwable ignored) {
          return false;
        }
      }
    };
  }
}
