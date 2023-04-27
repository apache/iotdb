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
package org.apache.iotdb.db.metadata.mnode.config.container;

import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.config.IConfigMNode;

import org.jetbrains.annotations.NotNull;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.emptySet;

public class ConfigMNodeContainer extends ConcurrentHashMap<String, IConfigMNode>
    implements IMNodeContainer<IConfigMNode> {

  private static final IMNodeContainer<IConfigMNode> EMPTY_CONTAINER =
      new ConfigMNodeContainer.EmptyContainer();

  public static IMNodeContainer<IConfigMNode> emptyMNodeContainer() {
    return EMPTY_CONTAINER;
  }

  private static class EmptyContainer extends AbstractMap<String, IConfigMNode>
      implements IMNodeContainer<IConfigMNode> {

    @Override
    public int size() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public boolean containsKey(Object key) {
      return false;
    }

    @Override
    public boolean containsValue(Object value) {
      return false;
    }

    @Override
    public IConfigMNode get(Object key) {
      return null;
    }

    @Override
    @NotNull
    public Set<String> keySet() {
      return emptySet();
    }

    @Override
    @NotNull
    public Collection<IConfigMNode> values() {
      return emptySet();
    }

    @NotNull
    public Set<Entry<String, IConfigMNode>> entrySet() {
      return emptySet();
    }

    @Override
    public boolean equals(Object o) {
      return o == this;
    }
  }
}
