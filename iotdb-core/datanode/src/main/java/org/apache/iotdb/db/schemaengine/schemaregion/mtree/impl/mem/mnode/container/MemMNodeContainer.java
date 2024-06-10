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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.container;

import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;

import javax.validation.constraints.NotNull;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

public class MemMNodeContainer extends KeyNullableConcurrentHashMap<String, IMemMNode>
    implements IMNodeContainer<IMemMNode> {

  private static final IMNodeContainer<IMemMNode> EMPTY_CONTAINER =
      new MemMNodeContainer.EmptyContainer();

  public static IMNodeContainer<IMemMNode> emptyMNodeContainer() {
    return EMPTY_CONTAINER;
  }

  private static class EmptyContainer extends AbstractMap<String, IMemMNode>
      implements IMNodeContainer<IMemMNode> {

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
    public IMemMNode get(Object key) {
      return null;
    }

    @Override
    @NotNull
    public Set<String> keySet() {
      return emptySet();
    }

    @Override
    @NotNull
    public Collection<IMemMNode> values() {
      return emptySet();
    }

    @NotNull
    public Set<Map.Entry<String, IMemMNode>> entrySet() {
      return emptySet();
    }

    @Override
    public boolean equals(Object o) {
      return o == this;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }
}
