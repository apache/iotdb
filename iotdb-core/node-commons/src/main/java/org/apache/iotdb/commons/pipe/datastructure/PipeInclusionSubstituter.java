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

package org.apache.iotdb.commons.pipe.datastructure;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class PipeInclusionSubstituter {
  private static final Map<String, Set<String>> SUBSTITUTION_MAP = new HashMap<>();

  static {
    SUBSTITUTION_MAP.put("all", new HashSet<>(Arrays.asList("data", "schema", "auth", "ttl")));
    SUBSTITUTION_MAP.put(
        "deletion",
        new HashSet<>(
            Arrays.asList(
                "schema.database.drop",
                "schema.template.drop",
                "auth.role.drop",
                "auth.user.drop",
                "ttl.unset")));
    SUBSTITUTION_MAP.put(
        "schema.deletion",
        new HashSet<>(Arrays.asList("schema.database.drop", "schema.template.drop")));
    SUBSTITUTION_MAP.put(
        "auth.deletion", new HashSet<>(Arrays.asList("auth.role.drop", "auth.user.drop")));
  }

  public static List<PartialPath> getPartialPaths(List<String> stringPrefixes)
      throws IllegalPathException {
    List<PartialPath> result;
    AtomicReference<IllegalPathException> exception = new AtomicReference<>();
    result =
        stringPrefixes.stream()
            .flatMap(
                prefix ->
                    SUBSTITUTION_MAP.getOrDefault(prefix, Collections.singleton(prefix)).stream())
            .map(
                inclusion -> {
                  try {
                    return new PartialPath(inclusion);
                  } catch (IllegalPathException e) {
                    exception.set(e);
                    return new PartialPath();
                  }
                })
            .collect(Collectors.toList());
    if (exception.get() != null) {
      throw exception.get();
    }
    return result;
  }

  private PipeInclusionSubstituter() {
    // Utility class
  }
}
