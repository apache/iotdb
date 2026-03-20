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

package org.apache.iotdb.commons.consensus.iotv2.consistency;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Durable partition-scoped mismatch description produced by logical snapshot compare.
 *
 * <p>The persisted string keeps backward compatibility with the legacy {@code treeKind@leafId}
 * format while allowing the new logical-snapshot workflow to optionally carry a narrowed key range
 * for each mismatched leaf.
 */
public final class LogicalMismatchScope {

  private static final String ENTRY_SEPARATOR = ",";
  private static final String TOKEN_SEPARATOR = "@";

  private LogicalMismatchScope() {}

  public enum RepairDirective {
    REPAIRABLE,
    FOLLOWER_EXTRA_TOMBSTONE
  }

  public static String serialize(Collection<Scope> scopes) {
    if (scopes == null || scopes.isEmpty()) {
      return null;
    }
    return scopes.stream()
        .filter(Objects::nonNull)
        .distinct()
        .sorted(Comparator.comparing(Scope::toPersistentString))
        .map(Scope::toPersistentString)
        .collect(Collectors.joining(ENTRY_SEPARATOR));
  }

  public static List<Scope> deserialize(String serialized) {
    if (serialized == null || serialized.trim().isEmpty()) {
      return Collections.emptyList();
    }

    LinkedHashSet<Scope> scopes = new LinkedHashSet<>();
    for (String rawEntry : serialized.split(ENTRY_SEPARATOR)) {
      String entry = rawEntry.trim();
      if (entry.isEmpty()) {
        continue;
      }
      String[] tokens = entry.split(TOKEN_SEPARATOR, -1);
      if (tokens.length < 2) {
        continue;
      }
      String treeKind = tokens[0];
      String leafId = tokens[1];
      String keyRangeStart = tokens.length >= 3 ? decodeNullable(tokens[2]) : null;
      String keyRangeEnd = tokens.length >= 4 ? decodeNullable(tokens[3]) : null;
      List<String> exactKeys =
          tokens.length >= 5 ? decodeStringList(tokens[4]) : Collections.emptyList();
      RepairDirective repairDirective =
          tokens.length >= 6 ? decodeRepairDirective(tokens[5]) : RepairDirective.REPAIRABLE;
      scopes.add(
          new Scope(treeKind, leafId, keyRangeStart, keyRangeEnd, exactKeys, repairDirective));
    }
    return new ArrayList<>(scopes);
  }

  private static String encodeNullable(String value) {
    if (value == null) {
      return "";
    }
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String decodeNullable(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
  }

  private static String encodeStringList(List<String> values) {
    if (values == null || values.isEmpty()) {
      return "";
    }
    return encodeNullable(String.join("\n", values));
  }

  private static List<String> decodeStringList(String value) {
    String decoded = decodeNullable(value);
    if (decoded == null || decoded.isEmpty()) {
      return Collections.emptyList();
    }
    LinkedHashSet<String> result = new LinkedHashSet<>();
    for (String key : decoded.split("\n")) {
      if (!key.isEmpty()) {
        result.add(key);
      }
    }
    return new ArrayList<>(result);
  }

  private static RepairDirective decodeRepairDirective(String value) {
    String decoded = decodeNullable(value);
    if (decoded == null || decoded.isEmpty()) {
      return RepairDirective.REPAIRABLE;
    }
    try {
      return RepairDirective.valueOf(decoded);
    } catch (IllegalArgumentException ignored) {
      return RepairDirective.REPAIRABLE;
    }
  }

  public static final class Scope {
    private final String treeKind;
    private final String leafId;
    private final String keyRangeStart;
    private final String keyRangeEnd;
    private final List<String> exactKeys;
    private final RepairDirective repairDirective;

    public Scope(String treeKind, String leafId) {
      this(treeKind, leafId, null, null, Collections.emptyList(), RepairDirective.REPAIRABLE);
    }

    public Scope(String treeKind, String leafId, String keyRangeStart, String keyRangeEnd) {
      this(
          treeKind,
          leafId,
          keyRangeStart,
          keyRangeEnd,
          Collections.emptyList(),
          RepairDirective.REPAIRABLE);
    }

    public Scope(
        String treeKind,
        String leafId,
        String keyRangeStart,
        String keyRangeEnd,
        List<String> exactKeys) {
      this(treeKind, leafId, keyRangeStart, keyRangeEnd, exactKeys, RepairDirective.REPAIRABLE);
    }

    public Scope(
        String treeKind,
        String leafId,
        String keyRangeStart,
        String keyRangeEnd,
        List<String> exactKeys,
        RepairDirective repairDirective) {
      this.treeKind = treeKind;
      this.leafId = leafId;
      this.keyRangeStart = keyRangeStart;
      this.keyRangeEnd = keyRangeEnd;
      this.exactKeys =
          exactKeys == null
              ? Collections.emptyList()
              : Collections.unmodifiableList(
                  exactKeys.stream()
                      .filter(Objects::nonNull)
                      .distinct()
                      .collect(Collectors.toList()));
      this.repairDirective = repairDirective == null ? RepairDirective.REPAIRABLE : repairDirective;
    }

    public String getTreeKind() {
      return treeKind;
    }

    public String getLeafId() {
      return leafId;
    }

    public String getKeyRangeStart() {
      return keyRangeStart;
    }

    public String getKeyRangeEnd() {
      return keyRangeEnd;
    }

    public List<String> getExactKeys() {
      return exactKeys;
    }

    public RepairDirective getRepairDirective() {
      return repairDirective;
    }

    public boolean isRepairable() {
      return repairDirective == RepairDirective.REPAIRABLE;
    }

    public String toPersistentString() {
      if (keyRangeStart == null
          && keyRangeEnd == null
          && exactKeys.isEmpty()
          && repairDirective == RepairDirective.REPAIRABLE) {
        return treeKind + TOKEN_SEPARATOR + leafId;
      }
      String base =
          treeKind
              + TOKEN_SEPARATOR
              + leafId
              + TOKEN_SEPARATOR
              + encodeNullable(keyRangeStart)
              + TOKEN_SEPARATOR
              + encodeNullable(keyRangeEnd)
              + TOKEN_SEPARATOR
              + encodeStringList(exactKeys);
      if (repairDirective == RepairDirective.REPAIRABLE) {
        return base;
      }
      return base + TOKEN_SEPARATOR + encodeNullable(repairDirective.name());
    }

    @Override
    public String toString() {
      return toPersistentString();
    }

    @Override
    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (!(object instanceof Scope)) {
        return false;
      }
      Scope that = (Scope) object;
      return Objects.equals(treeKind, that.treeKind)
          && Objects.equals(leafId, that.leafId)
          && Objects.equals(keyRangeStart, that.keyRangeStart)
          && Objects.equals(keyRangeEnd, that.keyRangeEnd)
          && Objects.equals(exactKeys, that.exactKeys)
          && repairDirective == that.repairDirective;
    }

    @Override
    public int hashCode() {
      return Objects.hash(treeKind, leafId, keyRangeStart, keyRangeEnd, exactKeys, repairDirective);
    }
  }
}
