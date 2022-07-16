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

package org.apache.iotdb.db.mpp.transformation.dag.util;

import java.util.Objects;

public class TransformUtils {

  public static int compare(CharSequence cs1, CharSequence cs2) {
    if (Objects.requireNonNull(cs1) == Objects.requireNonNull(cs2)) {
      return 0;
    }

    if (cs1.getClass() == cs2.getClass() && cs1 instanceof Comparable) {
      return ((Comparable<Object>) cs1).compareTo(cs2);
    }

    for (int i = 0, len = Math.min(cs1.length(), cs2.length()); i < len; i++) {
      char a = cs1.charAt(i);
      char b = cs2.charAt(i);
      if (a != b) {
        return a - b;
      }
    }

    return cs1.length() - cs2.length();
  }
}
