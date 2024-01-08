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

public class PipeInclusionNormalizer {
  private static final Map<String, Set<String>> SUBSTITUTION_MAP = new HashMap<>();
  private static final Set<PartialPath> LEGAL_PATHS = new HashSet<>();

  static {
    try {
      LEGAL_PATHS.add(new PartialPath("data.insert"));
      LEGAL_PATHS.add(new PartialPath("data.delete"));

      LEGAL_PATHS.add(new PartialPath("schema.database.create"));
      LEGAL_PATHS.add(new PartialPath("schema.database.alter"));
      LEGAL_PATHS.add(new PartialPath("schema.database.drop"));

      LEGAL_PATHS.add(new PartialPath("schema.timeseries.view.create"));
      LEGAL_PATHS.add(new PartialPath("schema.timeseries.view.alter"));
      LEGAL_PATHS.add(new PartialPath("schema.timeseries.view.drop"));

      LEGAL_PATHS.add(new PartialPath("schema.timeseries.ordinary.create"));
      LEGAL_PATHS.add(new PartialPath("schema.timeseries.ordinary.alter"));
      LEGAL_PATHS.add(new PartialPath("schema.timeseries.ordinary.delete"));

      LEGAL_PATHS.add(new PartialPath("schema.timeseries.template.create"));
      LEGAL_PATHS.add(new PartialPath("schema.timeseries.template.set"));
      LEGAL_PATHS.add(new PartialPath("schema.timeseries.template.unset"));
      LEGAL_PATHS.add(new PartialPath("schema.timeseries.template.alter"));
      LEGAL_PATHS.add(new PartialPath("schema.timeseries.template.drop"));
      LEGAL_PATHS.add(new PartialPath("schema.timeseries.template.activate"));
      LEGAL_PATHS.add(new PartialPath("schema.timeseries.template.deactivate"));

      LEGAL_PATHS.add(new PartialPath("schema.ttl.set"));
      LEGAL_PATHS.add(new PartialPath("schema.ttl.unset"));

      LEGAL_PATHS.add(new PartialPath("auth.role.create"));
      LEGAL_PATHS.add(new PartialPath("auth.role.drop"));
      LEGAL_PATHS.add(new PartialPath("auth.role.grant"));
      LEGAL_PATHS.add(new PartialPath("auth.role.revoke"));

      LEGAL_PATHS.add(new PartialPath("auth.user.create"));
      LEGAL_PATHS.add(new PartialPath("auth.user.alter"));
      LEGAL_PATHS.add(new PartialPath("auth.user.drop"));
      LEGAL_PATHS.add(new PartialPath("auth.user.grant"));
      LEGAL_PATHS.add(new PartialPath("auth.user.revoke"));

    } catch (IllegalPathException ignore) {
      // There won't be any exceptions here
    }

    SUBSTITUTION_MAP.put(
        "all",
        Collections.unmodifiableSet(new HashSet<>(Arrays.asList("data", "schema", "auth", "ttl"))));
    SUBSTITUTION_MAP.put(
        "deletion",
        Collections.unmodifiableSet(
            new HashSet<>(
                Arrays.asList(
                    "schema.database.drop",
                    "schema.template.drop",
                    "auth.role.drop",
                    "auth.user.drop",
                    "ttl.unset"))));
    SUBSTITUTION_MAP.put(
        "schema.deletion",
        Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("schema.database.drop", "schema.template.drop"))));
    SUBSTITUTION_MAP.put(
        "auth.deletion",
        Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("auth.role.drop", "auth.user.drop"))));
  }

  public static boolean allLegal(String prefixesRawStr) {
    try {
      return getPartialPaths(prefixesRawStr).stream()
          .allMatch(
              prefix ->
                  LEGAL_PATHS.stream().anyMatch(path -> path.overlapWithFullPathPrefix(prefix)));
    } catch (IllegalPathException e) {
      return false;
    }
  }

  public static List<PartialPath> getPartialPaths(String prefixesRawStr)
      throws IllegalPathException {
    if (prefixesRawStr.isEmpty()) {
      return Collections.emptyList();
    }

    List<PartialPath> result;
    AtomicReference<IllegalPathException> exception = new AtomicReference<>();
    result =
        Arrays.stream(prefixesRawStr.toLowerCase().replace(" ", "").split(","))
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

  private PipeInclusionNormalizer() {
    // Utility class
  }
}
