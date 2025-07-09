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

package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern;

import org.apache.tsfile.read.common.type.Type;

import static java.util.Objects.requireNonNull;

public class PhysicalValuePointer implements PhysicalValueAccessor {
  public static final int CLASSIFIER = -1;
  public static final int MATCH_NUMBER = -2;

  private final int sourceChannel;
  private final Type type;
  private final LogicalIndexNavigation logicalIndexNavigation;

  public PhysicalValuePointer(
      int sourceChannel, Type type, LogicalIndexNavigation logicalIndexNavigation) {
    this.sourceChannel = sourceChannel;
    this.type = requireNonNull(type, "type is null");
    this.logicalIndexNavigation =
        requireNonNull(logicalIndexNavigation, "logicalIndexNavigation is null");
  }

  public int getSourceChannel() {
    return sourceChannel;
  }

  public Type getType() {
    return type;
  }

  public LogicalIndexNavigation getLogicalIndexNavigation() {
    return logicalIndexNavigation;
  }
}
