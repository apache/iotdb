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

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class SkipOnSetupFailure implements TestRule {

  private static final String SET_UP_METHOD_NAME = "setUp";

  @Override
  public Statement apply(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() {
        try {
          base.evaluate();
        } catch (final Throwable e) {
          if (e.getStackTrace()[0].getMethodName().equals(SET_UP_METHOD_NAME)) {
            e.printStackTrace();
            throw new AssumptionViolatedException(
                String.format(
                    "Skipping test due to setup failure for %s@%s",
                    description.getClassName(), description.getMethodName()));
          }
        }
      }
    };
  }
}
