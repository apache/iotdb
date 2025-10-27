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

package org.apache.iotdb.commons.pipe.datastructure.pattern;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a union of multiple {@link PipePattern}s. A path is considered to match if it matches
 * any of the patterns in the collection.
 */
public class UnionPipePattern extends PipePattern {

  private final List<PipePattern> patterns;

  public UnionPipePattern(final List<PipePattern> patterns) {
    this.patterns = patterns;
  }

  @Override
  public boolean isSingle() {
    return patterns.size() == 1;
  }

  @Override
  public String getPattern() {
    return patterns.stream().map(PipePattern::getPattern).collect(Collectors.joining(","));
  }

  @Override
  public boolean isRoot() {
    return patterns.stream().anyMatch(PipePattern::isRoot);
  }

  @Override
  public boolean isLegal() {
    return patterns.stream().allMatch(PipePattern::isLegal);
  }

  @Override
  public boolean coversDb(final String db) {
    return patterns.stream().anyMatch(p -> p.coversDb(db));
  }

  @Override
  public boolean coversDevice(final String device) {
    return patterns.stream().anyMatch(p -> p.coversDevice(device));
  }

  @Override
  public boolean mayOverlapWithDb(final String db) {
    return patterns.stream().anyMatch(p -> p.mayOverlapWithDb(db));
  }

  @Override
  public boolean mayOverlapWithDevice(final String device) {
    return patterns.stream().anyMatch(p -> p.mayOverlapWithDevice(device));
  }

  @Override
  public boolean matchesMeasurement(final String device, final String measurement) {
    return patterns.stream().anyMatch(p -> p.matchesMeasurement(device, measurement));
  }

  @Override
  public String toString() {
    return "UnionPipePattern{" + "patterns=" + patterns + '}';
  }
}
