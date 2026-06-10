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

package org.apache.iotdb.calc.execution.operator.process.rowpattern.matcher;

import org.apache.iotdb.calc.i18n.CalcMessages;

import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ArrayView {
  public static final ArrayView EMPTY = new ArrayView(new int[] {}, 0);

  private final int[] array;
  private final int length;

  public ArrayView(int[] array, int length) {
    this.array = requireNonNull(array, CalcMessages.EXCEPTION_ARRAY_IS_NULL_BCF2EEB1);
    checkArgument(length >= 0, CalcMessages.EXCEPTION_USED_SLOTS_COUNT_IS_NEGATIVE_49F017C5);
    checkArgument(
        length <= array.length,
        CalcMessages.EXCEPTION_USED_SLOTS_COUNT_EXCEEDS_ARRAY_SIZE_6DCD8C7E);
    this.length = length;
  }

  public int get(int index) {
    checkArgument(
        index >= 0 && index < length, CalcMessages.EXCEPTION_ARRAY_INDEX_OUT_OF_BOUNDS_35C8A83F);
    return array[index];
  }

  public int length() {
    return length;
  }

  @VisibleForTesting
  public int[] toArray() {
    return Arrays.copyOf(array, length);
  }
}
