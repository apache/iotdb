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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/** Controls retry logic for test failures based on the {@link Retry} annotation. */
public class RetryRule implements TestRule {

  @Override
  public Statement apply(final Statement base, final Description description) {
    // Read the annotation on the method; if absent, do not retry (times = 1)
    final Retry retry = description.getAnnotation(Retry.class);
    final int times = (retry != null ? retry.times() : 1);
    return new RetryStatement(base, description, times);
  }

  private static class RetryStatement extends Statement {
    private final Statement base;
    private final Description description;
    private final int times;

    RetryStatement(final Statement base, final Description description, final int times) {
      this.base = base;
      this.description = description;
      this.times = times;
    }

    @Override
    public void evaluate() throws Throwable {
      Throwable lastThrowable;
      for (int i = 1; i <= times; i++) {
        try {
          base.evaluate();
          return; // Return immediately on success
        } catch (final Throwable t) {
          lastThrowable = t;
          System.err.printf(
              "[%s] run %d/%d failed: %s%n",
              description.getDisplayName(), i, times, t.getMessage());
          if (i == times) {
            // If it's the last attempt, and it still fails, throw the exception
            throw lastThrowable;
          }
          // Otherwise, continue to the next retry
        }
      }
    }
  }
}
