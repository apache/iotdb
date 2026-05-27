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

package com.timecho.iotdb.db.it.audit;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses ENTITY_STATUS_CHANGED audit log entries produced by ConfigNode's EventService and lets the
 * caller assert that every expected entity (Node / RegionGroup / per-DataNode Region /
 * ConsensusGroup) eventually reached a terminal "Running" / leader-elected state.
 *
 * <p>EventService aggregates state changes on a heartbeat tick, so intermediate transitions may be
 * collapsed non-deterministically between runs. Instead of expecting a fixed sequence, callers
 * verify that for each entity at least one audit log entry shows the desired terminal state.
 */
public class EntityStatusAuditLogChecker {

  private static final Logger LOGGER = LoggerFactory.getLogger(EntityStatusAuditLogChecker.class);

  // [RegionGroupStatistics]\t RegionGroup TConsensusGroupId(type:SchemaRegion, id:0), Region in
  // DataNode 1: <prev> -> <next>
  private static final Pattern REGION_PATTERN =
      Pattern.compile(
          "^\\[RegionGroupStatistics\\]\\s+RegionGroup TConsensusGroupId\\(type:(\\w+),\\s*id:(\\d+)\\),\\s*Region in DataNode\\s*(\\d+):.*->\\s*(.+?)\\s*$");

  // [RegionGroupStatistics]\t RegionGroup TConsensusGroupId(type:SchemaRegion, id:0): <prev> ->
  // <next>
  private static final Pattern REGION_GROUP_PATTERN =
      Pattern.compile(
          "^\\[RegionGroupStatistics\\]\\s+RegionGroup TConsensusGroupId\\(type:(\\w+),\\s*id:(\\d+)\\):.*->\\s*(.+?)\\s*$");

  // [NodeStatistics]\t nodeId{0}: <prev> -> <next>
  private static final Pattern NODE_PATTERN =
      Pattern.compile("^\\[NodeStatistics\\]\\s+nodeId\\{(\\d+)\\}:.*->\\s*(.+?)\\s*$");

  // [ConsensusGroupStatistics]\t TConsensusGroupId(type:SchemaRegion, id:0): <prev> -> <next>
  private static final Pattern CONSENSUS_GROUP_PATTERN =
      Pattern.compile(
          "^\\[ConsensusGroupStatistics\\]\\s+TConsensusGroupId\\(type:(\\w+),\\s*id:(\\d+)\\):.*->\\s*(.+?)\\s*$");

  // status=Running inside NodeStatistics{status=Running, ...}
  private static final Pattern NODE_STATUS_FIELD_PATTERN = Pattern.compile("status=(\\w+)");
  // leaderId=N inside ConsensusGroupStatistics{leaderId=N}
  private static final Pattern LEADER_ID_FIELD_PATTERN = Pattern.compile("leaderId=(-?\\d+)");

  /** Terminal status considered "ready" for Node, RegionGroup, and per-DataNode Region. */
  public static final String RUNNING = "Running";

  /** Identifies a RegionGroup / ConsensusGroup by (type, id), e.g. (SchemaRegion, 0). */
  public static final class GroupKey {
    final String type;
    final int id;

    public GroupKey(String type, int id) {
      this.type = type;
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof GroupKey)) {
        return false;
      }
      GroupKey that = (GroupKey) o;
      return id == that.id && type.equals(that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, id);
    }

    @Override
    public String toString() {
      return "TConsensusGroupId(type:" + type + ", id:" + id + ")";
    }
  }

  /** Identifies a Region replica by (type, id, dataNodeId). */
  public static final class RegionKey {
    final String type;
    final int id;
    final int dataNodeId;

    public RegionKey(String type, int id, int dataNodeId) {
      this.type = type;
      this.id = id;
      this.dataNodeId = dataNodeId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RegionKey)) {
        return false;
      }
      RegionKey that = (RegionKey) o;
      return id == that.id && dataNodeId == that.dataNodeId && type.equals(that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, id, dataNodeId);
    }

    @Override
    public String toString() {
      return "TConsensusGroupId(type:" + type + ", id:" + id + ") on DataNode " + dataNodeId;
    }
  }

  /**
   * Aggregated view of the observed terminal (right-hand side of "->") states for each tracked
   * entity. Each map collects every distinct terminal value, not just the latest one.
   */
  public static final class ParsedAuditLogs {
    final Map<Integer, Set<String>> nodeStatuses = new HashMap<>();
    final Map<GroupKey, Set<String>> regionGroupStatuses = new HashMap<>();
    final Map<RegionKey, Set<String>> regionStatuses = new HashMap<>();
    final Map<GroupKey, Set<Integer>> consensusGroupLeaders = new HashMap<>();
    int totalLines = 0;
    int unparsedLines = 0;

    public Set<Integer> nodeIds() {
      return Collections.unmodifiableSet(nodeStatuses.keySet());
    }

    public Set<GroupKey> regionGroups() {
      return Collections.unmodifiableSet(regionGroupStatuses.keySet());
    }

    public Set<RegionKey> regions() {
      return Collections.unmodifiableSet(regionStatuses.keySet());
    }

    public Set<GroupKey> consensusGroups() {
      return Collections.unmodifiableSet(consensusGroupLeaders.keySet());
    }
  }

  private EntityStatusAuditLogChecker() {}

  /**
   * Reads the given result set and parses every row whose log field looks like an entity-status
   * change (i.e. starts with "[NodeStatistics]", "[RegionGroupStatistics]" or
   * "[ConsensusGroupStatistics]"). Non-status rows in the result set are skipped silently, so the
   * caller may pass through an unfiltered SELECT * if filtering on the SQL side is awkward.
   *
   * @param resultSet result set produced by SELECT * ... ORDER BY TIME [ALIGN BY DEVICE]
   * @param logColumnIndex 1-based JDBC column index of the "log" field
   */
  public static ParsedAuditLogs parse(ResultSet resultSet, int logColumnIndex) throws SQLException {
    ParsedAuditLogs parsed = new ParsedAuditLogs();
    while (resultSet.next()) {
      String log = resultSet.getString(logColumnIndex);
      if (log == null
          || !(log.startsWith("[NodeStatistics]")
              || log.startsWith("[RegionGroupStatistics]")
              || log.startsWith("[ConsensusGroupStatistics]"))) {
        continue;
      }
      parsed.totalLines++;
      if (!tryParseLogLine(log, parsed)) {
        parsed.unparsedLines++;
        LOGGER.warn("Unparsed entity-status audit log line: {}", log);
      }
    }
    LOGGER.info(
        "Parsed {} entity-status audit log line(s) ({} unparsed). "
            + "Nodes={}, RegionGroups={}, Regions={}, ConsensusGroups={}",
        parsed.totalLines,
        parsed.unparsedLines,
        parsed.nodeStatuses.keySet(),
        parsed.regionGroupStatuses.keySet(),
        parsed.regionStatuses.keySet(),
        parsed.consensusGroupLeaders.keySet());
    return parsed;
  }

  /** Returns true when {@code log} matched one of the known patterns. */
  private static boolean tryParseLogLine(String log, ParsedAuditLogs parsed) {
    // Region pattern must be tried before RegionGroup pattern: both start with
    // "[RegionGroupStatistics]\t RegionGroup ..." but the Region variant adds
    // ", Region in DataNode N" which the RegionGroup regex would otherwise swallow.
    Matcher regionMatcher = REGION_PATTERN.matcher(log);
    if (regionMatcher.find()) {
      String type = regionMatcher.group(1);
      int id = Integer.parseInt(regionMatcher.group(2));
      int dataNodeId = Integer.parseInt(regionMatcher.group(3));
      String terminal = regionMatcher.group(4);
      parsed
          .regionStatuses
          .computeIfAbsent(new RegionKey(type, id, dataNodeId), k -> new HashSet<>())
          .add(terminal);
      return true;
    }
    Matcher regionGroupMatcher = REGION_GROUP_PATTERN.matcher(log);
    if (regionGroupMatcher.find()) {
      String type = regionGroupMatcher.group(1);
      int id = Integer.parseInt(regionGroupMatcher.group(2));
      String terminal = regionGroupMatcher.group(3);
      parsed
          .regionGroupStatuses
          .computeIfAbsent(new GroupKey(type, id), k -> new HashSet<>())
          .add(terminal);
      return true;
    }
    Matcher nodeMatcher = NODE_PATTERN.matcher(log);
    if (nodeMatcher.find()) {
      int nodeId = Integer.parseInt(nodeMatcher.group(1));
      String terminal = nodeMatcher.group(2);
      // Pull "Running" out of NodeStatistics{status=Running, ...}; keep raw text when "null".
      String status = terminal;
      Matcher statusMatcher = NODE_STATUS_FIELD_PATTERN.matcher(terminal);
      if (statusMatcher.find()) {
        status = statusMatcher.group(1);
      }
      parsed.nodeStatuses.computeIfAbsent(nodeId, k -> new HashSet<>()).add(status);
      return true;
    }
    Matcher consensusMatcher = CONSENSUS_GROUP_PATTERN.matcher(log);
    if (consensusMatcher.find()) {
      String type = consensusMatcher.group(1);
      int id = Integer.parseInt(consensusMatcher.group(2));
      String terminal = consensusMatcher.group(3);
      // -1 (no leader yet) is treated as "not yet elected"; any other id counts as elected.
      Matcher leaderMatcher = LEADER_ID_FIELD_PATTERN.matcher(terminal);
      if (leaderMatcher.find()) {
        int leaderId = Integer.parseInt(leaderMatcher.group(1));
        parsed
            .consensusGroupLeaders
            .computeIfAbsent(new GroupKey(type, id), k -> new HashSet<>())
            .add(leaderId);
      }
      return true;
    }
    return false;
  }

  /**
   * Fails the test with a multi-line diagnostic if any expected entity never reached the terminal
   * state. Empty expected collections are treated as "no constraint" for that entity type.
   */
  public static void assertAllEntitiesReady(
      ParsedAuditLogs parsed,
      Collection<Integer> expectedNodeIds,
      Collection<GroupKey> expectedRegionGroups,
      Collection<RegionKey> expectedRegions,
      Collection<GroupKey> expectedConsensusGroups) {
    List<String> failures = new ArrayList<>();

    for (Integer nodeId : new LinkedHashSet<>(expectedNodeIds)) {
      Set<String> observed = parsed.nodeStatuses.get(nodeId);
      if (observed == null || !observed.contains(RUNNING)) {
        failures.add(
            String.format(
                "Node nodeId{%d}: never reached %s (observed=%s)",
                nodeId, RUNNING, observed == null ? "[]" : observed));
      }
    }
    for (GroupKey key : new LinkedHashSet<>(expectedRegionGroups)) {
      Set<String> observed = parsed.regionGroupStatuses.get(key);
      if (observed == null || !observed.contains(RUNNING)) {
        failures.add(
            String.format(
                "RegionGroup %s: never reached %s (observed=%s)",
                key, RUNNING, observed == null ? "[]" : observed));
      }
    }
    for (RegionKey key : new LinkedHashSet<>(expectedRegions)) {
      Set<String> observed = parsed.regionStatuses.get(key);
      if (observed == null || !observed.contains(RUNNING)) {
        failures.add(
            String.format(
                "Region %s: never reached %s (observed=%s)",
                key, RUNNING, observed == null ? "[]" : observed));
      }
    }
    for (GroupKey key : new LinkedHashSet<>(expectedConsensusGroups)) {
      Set<Integer> observed = parsed.consensusGroupLeaders.get(key);
      boolean elected = observed != null && observed.stream().anyMatch(id -> id != -1);
      if (!elected) {
        failures.add(
            String.format(
                "ConsensusGroup %s: never elected a leader (observed=%s)",
                key, observed == null ? "[]" : observed));
      }
    }

    if (!failures.isEmpty()) {
      LOGGER.error("=========== ENTITY_STATUS_CHANGED audit log assertion FAILED ===========");
      for (String f : failures) {
        LOGGER.error("  {}", f);
      }
      LOGGER.error("=========================================================================");
      Assert.fail(
          "ENTITY_STATUS_CHANGED audit log assertion failed: " + String.join(" | ", failures));
    }
  }

  /** Convenience for the (very common) case where {@code types} all share Running as terminal. */
  public static List<GroupKey> groupKeys(int[] ids, String type) {
    List<GroupKey> result = new ArrayList<>(ids.length);
    for (int id : ids) {
      result.add(new GroupKey(type, id));
    }
    return result;
  }

  /**
   * Convenience for asserting every region of {@code regionGroupIds} on every {@code dataNodeIds}.
   */
  public static List<RegionKey> regionKeys(
      Collection<Integer> regionGroupIds, String type, Collection<Integer> dataNodeIds) {
    List<RegionKey> result = new ArrayList<>(regionGroupIds.size() * dataNodeIds.size());
    for (int rgId : regionGroupIds) {
      for (int nodeId : dataNodeIds) {
        result.add(new RegionKey(type, rgId, nodeId));
      }
    }
    return result;
  }

  /** Convenience for the simple "wrap an int[] as List" call site. */
  public static List<Integer> ints(int... ids) {
    return Arrays.stream(ids).boxed().collect(java.util.stream.Collectors.toList());
  }
}
