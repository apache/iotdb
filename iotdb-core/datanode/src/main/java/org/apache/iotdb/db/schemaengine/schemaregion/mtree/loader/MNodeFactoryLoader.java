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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader;

import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ServiceLoader;

@SuppressWarnings({"squid:S6548", "squid:3077"})
public class MNodeFactoryLoader {
  private volatile IMNodeFactory<ICachedMNode> cachedMNodeIMNodeFactory;
  private volatile IMNodeFactory<IMemMNode> memMNodeIMNodeFactory;

  public IMNodeFactory<ICachedMNode> getCachedMNodeIMNodeFactory() {
    if (cachedMNodeIMNodeFactory == null) {
      synchronized (this) {
        if (cachedMNodeIMNodeFactory == null) {
          loadCachedMNodeFactory();
        }
      }
    }
    return cachedMNodeIMNodeFactory;
  }

  public IMNodeFactory<IMemMNode> getMemMNodeIMNodeFactory() {
    if (memMNodeIMNodeFactory == null) {
      synchronized (this) {
        if (memMNodeIMNodeFactory == null) {
          loadMemMNodeFactory();
        }
      }
    }
    return memMNodeIMNodeFactory;
  }

  @SuppressWarnings("squid:S3740")
  private void loadCachedMNodeFactory() {
    ServiceLoader<IMNodeFactory> loader = ServiceLoader.load(IMNodeFactory.class);
    for (IMNodeFactory factory : loader) {
      if (isImplemented(factory, ICachedMNode.class)) {
        cachedMNodeIMNodeFactory = factory;
        return;
      }
    }
    throw new SchemaExecutionException("No implementation found for IMNodeFactory<ICacheMNode>");
  }

  @SuppressWarnings("squid:S3740")
  private void loadMemMNodeFactory() {
    ServiceLoader<IMNodeFactory> loader = ServiceLoader.load(IMNodeFactory.class);
    for (IMNodeFactory factory : loader) {
      if (isImplemented(factory, IMemMNode.class)) {
        memMNodeIMNodeFactory = factory;
        return;
      }
    }
    throw new SchemaExecutionException("No implementation found for IMNodeFactory<IMemMNode>");
  }

  public static boolean isImplemented(IMNodeFactory<?> factory, Class<?> targetType) {
    Type[] interfaces = factory.getClass().getGenericInterfaces();
    for (Type type : interfaces) {
      if (type instanceof ParameterizedType) {
        ParameterizedType parameterizedType = (ParameterizedType) type;
        Type[] typeArguments = parameterizedType.getActualTypeArguments();
        for (Type typeArgument : typeArguments) {
          if (typeArgument instanceof Class
              && targetType.isAssignableFrom((Class<?>) typeArgument)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private MNodeFactoryLoader() {}

  private static class MNodeFactoryLoaderHolder {
    private static final MNodeFactoryLoader INSTANCE = new MNodeFactoryLoader();

    private MNodeFactoryLoaderHolder() {}
  }

  public static MNodeFactoryLoader getInstance() {
    return MNodeFactoryLoaderHolder.INSTANCE;
  }
}
