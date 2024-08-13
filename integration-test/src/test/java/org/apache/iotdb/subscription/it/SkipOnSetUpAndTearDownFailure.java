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
import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;

public class SkipOnSetUpAndTearDownFailure implements TestRule {

  private final String setUpMethodName;
  private final String tearDownMethodName;

  /**
   * @param setUpMethodName Should be exactly the same as the method name decorated with @Before.
   * @param tearDownMethodName Should be exactly the same as the method name decorated with @After.
   */
  public SkipOnSetUpAndTearDownFailure(
      @NonNull final String setUpMethodName, @NonNull final String tearDownMethodName) {
    this.setUpMethodName = setUpMethodName;
    this.tearDownMethodName = tearDownMethodName;
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          base.evaluate();
        } catch (final Throwable e) {
          // Pay attention to the situation of MultipleFailureException...
          if ((e instanceof MultipleFailureException
                  && ((MultipleFailureException) e)
                      .getFailures().stream().allMatch(this::isExceptionInSetUpOrTearDown))
              || isExceptionInSetUpOrTearDown(e)) {
            Assume.assumeTrue(
                String.format(
                    "Skipping test due to setup or tearDown failure for %s#%s",
                    description.getClassName(), description.getMethodName()),
                false);
          }

          // Re-throw the exception (which means the test has failed).
          throw e;

          // Regardless of the circumstances, the method decorated with @After will always be
          // executed.
        }
      }

      private boolean isExceptionInSetUpOrTearDown(final Throwable e) {
        // Trace back the exception stack to determine whether the exception was thrown during the
        // setUp or tearDown phase.
        for (final StackTraceElement stackTraceElement : e.getStackTrace()) {
          if (setUpMethodName.equals(stackTraceElement.getMethodName())
              && description.getClassName().equals(stackTraceElement.getClassName())
              && isMethodAnnotationWithBefore(stackTraceElement.getMethodName())) {
            e.printStackTrace();
            return true;
          }

          if (tearDownMethodName.equals(stackTraceElement.getMethodName())
              && description.getClassName().equals(stackTraceElement.getClassName())
              && isMethodAnnotationWithAfter(stackTraceElement.getMethodName())) {
            e.printStackTrace();
            return true;
          }
        }
        return false;
      }

      private boolean isMethodAnnotationWithBefore(final String methodName) {
        try {
          final Method method = description.getTestClass().getDeclaredMethod(methodName);
          return method.isAnnotationPresent(org.junit.Before.class);
        } catch (final Throwable ignored) {
          return false;
        }
      }

      private boolean isMethodAnnotationWithAfter(final String methodName) {
        try {
          final Method method = description.getTestClass().getDeclaredMethod(methodName);
          return method.isAnnotationPresent(org.junit.After.class);
        } catch (final Throwable ignored) {
          return false;
        }
      }
    };
  }
}
