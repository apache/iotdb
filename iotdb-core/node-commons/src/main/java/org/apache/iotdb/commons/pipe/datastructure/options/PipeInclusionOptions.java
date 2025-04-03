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

package org.apache.iotdb.commons.pipe.datastructure.options;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_INCLUSION_KEY;

public class PipeInclusionOptions {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeInclusionOptions.class);
  public static final List<PartialPath> treeOnlySyncPrefixes =
      Collections.singletonList(new PartialPath(new String[] {"schema", "timeseries"}));
  public static final List<PartialPath> tableOnlySyncPrefixes =
      Collections.unmodifiableList(
          Arrays.asList(
              new PartialPath(new String[] {"schema", "table"}),
              new PartialPath(new String[] {"auth", "role", "grant", "table"}),
              new PartialPath(new String[] {"auth", "role", "revoke", "table"}),
              new PartialPath(new String[] {"auth", "user", "grant", "table"}),
              new PartialPath(new String[] {"auth", "user", "revoke", "table"})));
  public static final String ALL = "all";

  private static final Set<PartialPath> OPTIONS = new HashSet<>();
  private static final Map<String, Set<String>> ALIAS_OPTIONS_MAP = new HashMap<>();

  static {
    try {
      OPTIONS.add(new PartialPath("data.insert"));
      OPTIONS.add(new PartialPath("data.delete"));

      OPTIONS.add(new PartialPath("schema.database.create"));
      OPTIONS.add(new PartialPath("schema.database.alter"));
      OPTIONS.add(new PartialPath("schema.database.drop"));

      OPTIONS.add(new PartialPath("schema.timeseries.view.create"));
      OPTIONS.add(new PartialPath("schema.timeseries.view.alter"));
      OPTIONS.add(new PartialPath("schema.timeseries.view.drop"));

      OPTIONS.add(new PartialPath("schema.timeseries.ordinary.create"));
      OPTIONS.add(new PartialPath("schema.timeseries.ordinary.alter"));
      OPTIONS.add(new PartialPath("schema.timeseries.ordinary.drop"));

      OPTIONS.add(new PartialPath("schema.timeseries.template.create"));
      OPTIONS.add(new PartialPath("schema.timeseries.template.set"));
      OPTIONS.add(new PartialPath("schema.timeseries.template.unset"));
      OPTIONS.add(new PartialPath("schema.timeseries.template.alter"));
      OPTIONS.add(new PartialPath("schema.timeseries.template.drop"));
      OPTIONS.add(new PartialPath("schema.timeseries.template.activate"));
      OPTIONS.add(new PartialPath("schema.timeseries.template.deactivate"));

      // Table
      OPTIONS.add(new PartialPath("schema.table.create"));
      OPTIONS.add(new PartialPath("schema.table.alter"));
      OPTIONS.add(new PartialPath("schema.table.drop"));

      OPTIONS.add(new PartialPath("schema.ttl"));

      OPTIONS.add(new PartialPath("auth.role.create"));
      OPTIONS.add(new PartialPath("auth.role.drop"));
      OPTIONS.add(new PartialPath("auth.role.grant"));
      OPTIONS.add(new PartialPath("auth.role.revoke"));

      OPTIONS.add(new PartialPath("auth.user.create"));
      OPTIONS.add(new PartialPath("auth.user.alter"));
      OPTIONS.add(new PartialPath("auth.user.drop"));
      OPTIONS.add(new PartialPath("auth.user.grant"));
      OPTIONS.add(new PartialPath("auth.user.revoke"));

      // Table auth
      OPTIONS.add(new PartialPath("auth.role.grant.table"));
      OPTIONS.add(new PartialPath("auth.role.revoke.table"));

      OPTIONS.add(new PartialPath("auth.user.grant.table"));
      OPTIONS.add(new PartialPath("auth.user.revoke.table"));
    } catch (final IllegalPathException e) {
      LOGGER.error("Illegal path encountered when initializing LEGAL_OPTIONS.", e);
    }

    ALIAS_OPTIONS_MAP.put(
        ALL, Collections.unmodifiableSet(new HashSet<>(Arrays.asList("data", "schema", "auth"))));
    ALIAS_OPTIONS_MAP.put(
        "delete",
        Collections.unmodifiableSet(
            new HashSet<>(
                Arrays.asList(
                    "data.delete",
                    "schema.database.drop",
                    "schema.timeseries.ordinary.delete",
                    "schema.timeseries.view.drop",
                    "schema.timeseries.template.drop",
                    "schema.timeseries.template.unset",
                    "schema.timeseries.template.deactivate",
                    "auth.role.drop",
                    "auth.role.revoke",
                    "auth.user.drop",
                    "auth.user.revoke"))));
    ALIAS_OPTIONS_MAP.put(
        "schema.delete",
        Collections.unmodifiableSet(
            new HashSet<>(
                Arrays.asList(
                    "schema.database.drop",
                    "schema.timeseries.ordinary.delete",
                    "schema.timeseries.view.drop",
                    "schema.timeseries.template.drop",
                    "schema.timeseries.template.unset",
                    "schema.timeseries.template.deactivate"))));
    ALIAS_OPTIONS_MAP.put(
        "auth.delete",
        Collections.unmodifiableSet(
            new HashSet<>(
                Arrays.asList(
                    "auth.role.drop", "auth.role.revoke", "auth.user.drop", "auth.user.revoke"))));
  }

  public static boolean hasAtLeastOneOption(
      final String inclusionString, final String exclusionString) {
    try {
      final Set<PartialPath> inclusion = parseOptions(inclusionString);
      final Set<PartialPath> exclusion = parseOptions(exclusionString);

      final Set<PartialPath> options = new HashSet<>();
      inclusion.forEach(
          option ->
              options.addAll(
                  OPTIONS.stream()
                      .filter(path -> path.overlapWithFullPathPrefix(option))
                      .collect(Collectors.toSet())));
      exclusion.forEach(
          option ->
              options.removeAll(
                  OPTIONS.stream()
                      .filter(path -> path.overlapWithFullPathPrefix(option))
                      .collect(Collectors.toSet())));
      return !options.isEmpty();
    } catch (final IllegalPathException e) {
      LOGGER.warn(
          "Illegal options (inclusion: {}, exclusion: {}) parsed "
              + "when checking if at least one option is present: {}",
          inclusionString,
          exclusionString,
          e.getMessage(),
          e);
      return false;
    }
  }

  public static boolean optionsAreAllLegal(final String options) {
    try {
      return parseOptions(options).stream()
          .allMatch(
              prefix -> OPTIONS.stream().anyMatch(path -> path.overlapWithFullPathPrefix(prefix)));
    } catch (final IllegalPathException e) {
      LOGGER.warn(
          "Illegal options {} parsed when checking if all options are legal: {}",
          options,
          e.getMessage(),
          e);
      return false;
    }
  }

  public static String getInclusionString(final PipeParameters parameters) {
    return parameters.getStringOrDefault(
        Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
        EXTRACTOR_INCLUSION_DEFAULT_VALUE);
  }

  public static String getExclusionString(final PipeParameters parameters) {
    return parameters.getStringOrDefault(
        Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
        EXTRACTOR_EXCLUSION_DEFAULT_VALUE);
  }

  public static Set<PartialPath> parseOptions(final String optionsString)
      throws IllegalPathException {
    if (optionsString.isEmpty()) {
      return Collections.emptySet();
    }

    final AtomicReference<IllegalPathException> exception = new AtomicReference<>();
    final Set<PartialPath> options =
        Arrays.stream(optionsString.toLowerCase().replace(" ", "").split(","))
            .flatMap(
                prefix ->
                    ALIAS_OPTIONS_MAP.getOrDefault(prefix, Collections.singleton(prefix)).stream())
            .map(
                inclusion -> {
                  try {
                    return new PartialPath(inclusion);
                  } catch (final IllegalPathException e) {
                    exception.set(e);
                    return new PartialPath();
                  }
                })
            .collect(Collectors.toSet());
    if (exception.get() != null) {
      throw exception.get();
    }
    return options;
  }

  private PipeInclusionOptions() {
    // Utility class
  }
}
