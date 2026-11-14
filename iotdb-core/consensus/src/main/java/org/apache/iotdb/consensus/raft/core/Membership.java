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

package org.apache.iotdb.consensus.raft.core;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Immutable cluster configuration: voters, learners, their endpoints and the log index of the
 * CONFIG entry that produced it (-1 for the bootstrap configuration).
 */
public final class Membership {

  private final Set<Integer> voters;
  private final Set<Integer> learners;
  private final Map<Integer, TEndPoint> endpoints;
  private final long configIndex;

  public Membership(
      Collection<Integer> voters,
      Collection<Integer> learners,
      Map<Integer, TEndPoint> endpoints,
      long configIndex) {
    this.voters = Collections.unmodifiableSet(new TreeSet<>(voters));
    this.learners = Collections.unmodifiableSet(new TreeSet<>(learners));
    this.endpoints = Collections.unmodifiableMap(new HashMap<>(endpoints));
    this.configIndex = configIndex;
  }

  /** Bootstrap configuration with the given voters and no endpoints (tests / in-memory). */
  public static Membership ofVoters(Collection<Integer> voters) {
    return new Membership(voters, Collections.emptySet(), Collections.emptyMap(), -1);
  }

  public static Membership ofVoters(Map<Integer, TEndPoint> voterEndpoints) {
    return new Membership(voterEndpoints.keySet(), Collections.emptySet(), voterEndpoints, -1);
  }

  /** The empty configuration of a learner that has not yet received a CONFIG entry. */
  public static Membership empty() {
    return new Membership(
        Collections.emptySet(), Collections.emptySet(), Collections.emptyMap(), -1);
  }

  public Set<Integer> voters() {
    return voters;
  }

  public Set<Integer> learners() {
    return learners;
  }

  public Map<Integer, TEndPoint> endpoints() {
    return endpoints;
  }

  public TEndPoint endpointOf(int nodeId) {
    return endpoints.get(nodeId);
  }

  public long configIndex() {
    return configIndex;
  }

  public boolean isVoter(int nodeId) {
    return voters.contains(nodeId);
  }

  public boolean isMember(int nodeId) {
    return voters.contains(nodeId) || learners.contains(nodeId);
  }

  public boolean isEmpty() {
    return voters.isEmpty() && learners.isEmpty();
  }

  /** Majority of the voters (n/2 + 1). */
  public int majority() {
    return voters.size() / 2 + 1;
  }

  /** All members except {@code self}, voters first then learners, ascending ids. */
  public Set<Integer> peersOf(int self) {
    Set<Integer> out = new TreeSet<>(voters);
    out.addAll(learners);
    out.remove(self);
    return out;
  }

  public Membership withVoter(int nodeId, TEndPoint endPoint, long configIndex) {
    Set<Integer> v = new TreeSet<>(voters);
    Set<Integer> l = new TreeSet<>(learners);
    Map<Integer, TEndPoint> e = new HashMap<>(endpoints);
    v.add(nodeId);
    l.remove(nodeId);
    if (endPoint != null) {
      e.put(nodeId, endPoint);
    }
    return new Membership(v, l, e, configIndex);
  }

  public Membership withLearner(int nodeId, TEndPoint endPoint, long configIndex) {
    Set<Integer> v = new TreeSet<>(voters);
    Set<Integer> l = new TreeSet<>(learners);
    Map<Integer, TEndPoint> e = new HashMap<>(endpoints);
    v.remove(nodeId);
    l.add(nodeId);
    if (endPoint != null) {
      e.put(nodeId, endPoint);
    }
    return new Membership(v, l, e, configIndex);
  }

  public Membership without(int nodeId, long configIndex) {
    Set<Integer> v = new TreeSet<>(voters);
    Set<Integer> l = new TreeSet<>(learners);
    Map<Integer, TEndPoint> e = new HashMap<>(endpoints);
    v.remove(nodeId);
    l.remove(nodeId);
    e.remove(nodeId);
    return new Membership(v, l, e, configIndex);
  }

  public Membership withConfigIndex(long configIndex) {
    return new Membership(voters, learners, endpoints, configIndex);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Membership)) {
      return false;
    }
    Membership m = (Membership) o;
    return configIndex == m.configIndex
        && voters.equals(m.voters)
        && learners.equals(m.learners)
        && endpoints.equals(m.endpoints);
  }

  @Override
  public int hashCode() {
    return Objects.hash(voters, learners, configIndex);
  }

  @Override
  public String toString() {
    return "Membership{voters=" + voters + ", learners=" + learners + ", cfg=" + configIndex + '}';
  }
}
