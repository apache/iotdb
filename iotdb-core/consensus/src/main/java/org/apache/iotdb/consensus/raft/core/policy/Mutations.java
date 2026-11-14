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

package org.apache.iotdb.consensus.raft.core.policy;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Test seam for the mutation-control convention of the Python reference tests: a named set of
 * deliberately wrong decisions. Production always runs with {@link #NONE}. The kernel consults it
 * at exactly the decision points {@code node.py} guards with {@code self.mut(name)}, so the 28
 * mutation cases port one-to-one. Like {@link org.apache.iotdb.consensus.raft.core.FaultInjector}
 * it is inert unless a test enables a name.
 */
public final class Mutations {

  public static final Mutations NONE = new Mutations(Collections.emptySet());

  private final Set<String> names;

  public Mutations(Set<String> names) {
    this.names = Collections.unmodifiableSet(new HashSet<>(names));
  }

  public static Mutations of(String... names) {
    Set<String> s = new HashSet<>();
    Collections.addAll(s, names);
    return new Mutations(s);
  }

  public boolean has(String name) {
    return names.contains(name);
  }

  public boolean isEmpty() {
    return names.isEmpty();
  }

  @Override
  public String toString() {
    return "Mutations" + names;
  }
}
