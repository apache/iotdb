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

package org.apache.iotdb.db.queryengine.execution.warnings;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DefaultWarningCollector implements WarningCollector {
  @GuardedBy("this")
  private final Set<IoTDBWarning> warnings = new LinkedHashSet<>();

  private final int maxWarnings;

  public DefaultWarningCollector(int maxWarnings) {
    this.maxWarnings = maxWarnings;
  }

  @Override
  public synchronized void add(IoTDBWarning warning) {
    requireNonNull(warning, "warning is null");
    if (warnings.size() < maxWarnings) {
      warnings.add(warning);
    }
  }

  @Override
  public synchronized List<IoTDBWarning> getWarnings() {
    return ImmutableList.copyOf(warnings);
  }
}
