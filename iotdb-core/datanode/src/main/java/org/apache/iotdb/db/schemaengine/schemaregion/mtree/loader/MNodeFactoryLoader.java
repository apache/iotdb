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
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.MNodeFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;

import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@SuppressWarnings({"squid:S6548", "squid:3077"})
public class MNodeFactoryLoader {

  private final List<String> scanPackages = new ArrayList<>();
  private String env;

  @SuppressWarnings("java:S3077")
  private volatile IMNodeFactory<ICachedMNode> cachedMNodeIMNodeFactory;

  @SuppressWarnings("java:S3077")
  private volatile IMNodeFactory<IMemMNode> memMNodeIMNodeFactory;

  private MNodeFactoryLoader() {
    addScanPackage("org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.factory");
    addScanPackage("org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.factory");
    setEnv(SchemaConstant.DEFAULT_MNODE_FACTORY_ENV);
  }

  public void addScanPackage(String scanPackage) {
    scanPackages.add(scanPackage);
  }

  public void setEnv(String env) {
    this.env = env;
  }

  @SuppressWarnings("squid:S3740")
  public IMNodeFactory<ICachedMNode> getCachedMNodeIMNodeFactory() {
    if (cachedMNodeIMNodeFactory == null) {
      synchronized (this) {
        if (cachedMNodeIMNodeFactory == null) {
          cachedMNodeIMNodeFactory = loadMNodeFactory(ICachedMNode.class);
        }
      }
    }
    return cachedMNodeIMNodeFactory;
  }

  @SuppressWarnings("squid:S3740")
  public IMNodeFactory<IMemMNode> getMemMNodeIMNodeFactory() {
    if (memMNodeIMNodeFactory == null) {
      synchronized (this) {
        if (memMNodeIMNodeFactory == null) {
          memMNodeIMNodeFactory = loadMNodeFactory(IMemMNode.class);
        }
      }
    }
    return memMNodeIMNodeFactory;
  }

  @SuppressWarnings("squid:S3740")
  private IMNodeFactory loadMNodeFactory(Class<?> nodeType) {
    Reflections reflections =
        new Reflections(
            new ConfigurationBuilder().forPackages(scanPackages.toArray(new String[0])));
    Set<Class<?>> nodeFactorySet = reflections.getTypesAnnotatedWith(MNodeFactory.class);
    for (Class<?> nodeFactory : nodeFactorySet) {
      if (isGenericMatch(nodeFactory, nodeType) && isEnvMatch(nodeFactory, env)) {
        try {
          return (IMNodeFactory) nodeFactory.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
          throw new SchemaExecutionException(e);
        }
      }
    }
    // if no satisfied MNodeFactory in customer env found, use default env
    for (Class<?> nodeFactory : nodeFactorySet) {
      if (isGenericMatch(nodeFactory, nodeType)
          && isEnvMatch(nodeFactory, SchemaConstant.DEFAULT_MNODE_FACTORY_ENV)) {
        try {
          return (IMNodeFactory) nodeFactory.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
          throw new SchemaExecutionException(e);
        }
      }
    }
    throw new SchemaExecutionException("No satisfied MNodeFactory found");
  }

  public boolean isGenericMatch(Class<?> factory, Class<?> targetType) {
    Type[] interfaces = factory.getGenericInterfaces();
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

  private boolean isEnvMatch(Class<?> factory, String env) {
    MNodeFactory annotationInfo = factory.getAnnotation(MNodeFactory.class);
    return annotationInfo.env().equals(env);
  }

  private static class MNodeFactoryLoaderHolder {
    private static final MNodeFactoryLoader INSTANCE = new MNodeFactoryLoader();

    private MNodeFactoryLoaderHolder() {}
  }

  public static MNodeFactoryLoader getInstance() {
    return MNodeFactoryLoaderHolder.INSTANCE;
  }
}
