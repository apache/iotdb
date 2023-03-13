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

package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.metadata.MetadataConstant;

import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class SchemaRegionLoader {
  private static final Logger logger = LoggerFactory.getLogger(SchemaEngine.class);

  private static final String PACKAGE_NAME = "org.apache.iotdb.db.metadata";

  private final Map<String, Constructor<ISchemaRegion>> constructorMap = new ConcurrentHashMap<>();

  private String currentMode;

  private Constructor<ISchemaRegion> currentConstructor;

  @SuppressWarnings("unchecked")
  SchemaRegionLoader() {
    Reflections reflections =
        new Reflections(
            new ConfigurationBuilder()
                .forPackages(PACKAGE_NAME)
                .filterInputsBy(new FilterBuilder().includePackage(PACKAGE_NAME)));

    Set<Class<?>> annotatedSchemaRegionSet = reflections.getTypesAnnotatedWith(SchemaRegion.class);

    for (Class<?> annotatedSchemaRegion : annotatedSchemaRegionSet) {
      boolean isSchemaRegion = false;
      for (Class<?> interfaces : annotatedSchemaRegion.getInterfaces()) {
        if (interfaces == ISchemaRegion.class) {
          isSchemaRegion = true;
          break;
        }
      }
      if (!isSchemaRegion) {
        logger.warn(
            String.format(
                "Class %s is not a subclass of ISchemaRegion.", annotatedSchemaRegion.getName()));
        continue;
      }
      SchemaRegion annotationInfo = annotatedSchemaRegion.getAnnotation(SchemaRegion.class);
      constructorMap.compute(
          annotationInfo.mode(),
          (k, v) -> {
            if (v == null) {
              try {
                return (Constructor<ISchemaRegion>)
                    annotatedSchemaRegion.getConstructor(ISchemaRegionParams.class);
              } catch (NoSuchMethodException e) {
                logger.error(e.getMessage(), e);
                return null;
              }
            }
            logger.warn(
                "Duplicated SchemaRegion implementation, {} and {}, with same mode name [{}]",
                v.getClass().getName(),
                annotatedSchemaRegion.getName(),
                k);
            return v;
          });
    }
  }

  void init(String schemaEngineMode) {
    Constructor<ISchemaRegion> constructor = constructorMap.get(schemaEngineMode);
    if (constructor == null) {
      logger.warn(
          "There's no SchemaRegion implementation with target mode {}. Use default mode {}",
          schemaEngineMode,
          MetadataConstant.DEFAULT_SCHEMA_ENGINE_MODE);
      currentMode = MetadataConstant.DEFAULT_SCHEMA_ENGINE_MODE;
      currentConstructor = constructorMap.get(currentMode);
    } else {
      currentMode = schemaEngineMode;
      currentConstructor = constructor;
    }
  }

  ISchemaRegion createSchemaRegion(ISchemaRegionParams schemaRegionParams)
      throws MetadataException {
    try {
      return currentConstructor.newInstance(schemaRegionParams);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      logger.warn(e.getMessage(), e);
      throw new MetadataException(e);
    }
  }

  void clear() {
    currentMode = null;
    currentConstructor = null;
  }
}
