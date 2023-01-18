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
package org.apache.iotdb.db.metadata.mnode.container;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.CachedMNodeContainer;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngineMode;

import org.jetbrains.annotations.NotNull;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

public class MNodeContainers {

  @SuppressWarnings("rawtypes")
  private static final IMNodeContainer EMPTY_CONTAINER = new EmptyContainer();

  @SuppressWarnings("unchecked")
  public static IMNodeContainer emptyMNodeContainer() {
    return EMPTY_CONTAINER;
  }

  public static boolean isEmptyContainer(IMNodeContainer container) {
    return EMPTY_CONTAINER.equals(container);
  }

  public static IMNodeContainer getNewMNodeContainer() {
    if (IoTDBDescriptor.getInstance()
        .getConfig()
        .getSchemaEngineMode()
        .equals(SchemaEngineMode.Schema_File.toString())) {
      return new CachedMNodeContainer();
    } else {
      return new MNodeContainerMapImpl();
    }
  }

  private static class EmptyContainer extends AbstractMap<String, IMNode>
      implements IMNodeContainer {

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
    public IMNode get(Object key) {
      return null;
    }

    @Override
    @NotNull
    public Set<String> keySet() {
      return emptySet();
    }

    @Override
    @NotNull
    public Collection<IMNode> values() {
      return emptySet();
    }

    @NotNull
    public Set<Map.Entry<String, IMNode>> entrySet() {
      return emptySet();
    }

    @Override
    public boolean equals(Object o) {
      return o == this;
    }
  }
}
