/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.utils;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

public final class MoreLists {
  public static <T> List<List<T>> listOfListsCopy(List<List<T>> lists) {
    return lists.stream().map(ImmutableList::copyOf).collect(toImmutableList());
  }

  public static <T> List<T> filteredCopy(Iterable<T> elements, Predicate<T> predicate) {
    requireNonNull(elements, DataNodeQueryMessages.EXCEPTION_ELEMENTS_IS_NULL_3451C1DA);
    requireNonNull(predicate, DataNodeQueryMessages.EXCEPTION_PREDICATE_IS_NULL_22E687A9);
    return stream(elements).filter(predicate).collect(toImmutableList());
  }

  public static <T, R> List<R> mappedCopy(Iterable<T> elements, Function<T, R> mapper) {
    requireNonNull(elements, DataNodeQueryMessages.EXCEPTION_ELEMENTS_IS_NULL_3451C1DA);
    requireNonNull(mapper, DataNodeQueryMessages.EXCEPTION_MAPPER_IS_NULL_1D7789D1);
    return stream(elements).map(mapper).collect(toImmutableList());
  }

  public static <T> List<T> nElements(int n, IntFunction<T> function) {
    checkArgument(
        n >= 0, DataNodeQueryMessages.EXCEPTION_N_MUST_BE_GREATER_THAN_OR_EQUAL_TO_ZERO_C4CE8BF0);
    requireNonNull(function, DataNodeQueryMessages.EXCEPTION_FUNCTION_IS_NULL_E0FA4B62);
    return IntStream.range(0, n).mapToObj(function).collect(toImmutableList());
  }

  private MoreLists() {}
}
