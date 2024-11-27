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

package org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.comparator;

import org.apache.tsfile.utils.Binary;

public class DescJoinKeyComparator implements JoinKeyComparator {

  private static final DescJoinKeyComparator INSTANCE = new DescJoinKeyComparator();

  private DescJoinKeyComparator() {
    // hide constructor
  }

  @Override
  public boolean lessThan(int left, int right) {
    return left > right;
  }

  @Override
  public boolean lessThan(long left, long right) {
    return left > right;
  }

  @Override
  public boolean lessThan(float left, float right) {
    return left > right;
  }

  @Override
  public boolean lessThan(double left, double right) {
    return left > right;
  }

  @Override
  public boolean lessThan(Binary left, Binary right) {
    return left.compareTo(right) > 0;
  }

  @Override
  public boolean equalsTo(int left, int right) {
    return left == right;
  }

  @Override
  public boolean equalsTo(long left, long right) {
    return left == right;
  }

  @Override
  public boolean equalsTo(float left, float right) {
    return left == right;
  }

  @Override
  public boolean equalsTo(double left, double right) {
    return left == right;
  }

  @Override
  public boolean equalsTo(Binary left, Binary right) {
    return left.equals(right);
  }

  @Override
  public boolean lessThanOrEqual(int left, int right) {
    return left >= right;
  }

  @Override
  public boolean lessThanOrEqual(long left, long right) {
    return left >= right;
  }

  @Override
  public boolean lessThanOrEqual(float left, float right) {
    return left >= right;
  }

  @Override
  public boolean lessThanOrEqual(double left, double right) {
    return left >= right;
  }

  @Override
  public boolean lessThanOrEqual(Binary left, Binary right) {
    return left.compareTo(right) >= 0;
  }

  public static DescJoinKeyComparator getInstance() {
    return INSTANCE;
  }
}
