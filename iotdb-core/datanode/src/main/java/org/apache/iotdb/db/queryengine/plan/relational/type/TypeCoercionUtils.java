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

package org.apache.iotdb.db.queryengine.plan.relational.type;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.read.common.type.IntType;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.common.type.Type;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class TypeCoercionUtils {

  private static final Map<Type, Set<Type>> typeCoercionMap;

  static {
    typeCoercionMap = ImmutableMap.of(IntType.INT32, ImmutableSet.of(LongType.INT64));
  }

  public static boolean canCoerceTo(Type from, Type to) {
    if (from.equals(to)) {
      return true;
    }
    return typeCoercionMap.getOrDefault(from, Collections.emptySet()).contains(to);
  }
}
