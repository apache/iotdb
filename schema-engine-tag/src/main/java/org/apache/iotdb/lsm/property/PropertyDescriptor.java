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
package org.apache.iotdb.lsm.property;

import org.apache.iotdb.lsm.annotation.DeletionProcess;
import org.apache.iotdb.lsm.annotation.InsertionProcess;
import org.apache.iotdb.lsm.annotation.QueryProcess;

import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PropertyDescriptor {

  public static Property getProperty(String packageName) throws Exception {
    Reflections reflections =
        new Reflections(
            new ConfigurationBuilder()
                .forPackage(packageName)
                .filterInputsBy(new FilterBuilder().includePackage(packageName)));
    Property property = new Property();
    setDeletionLevelProcess(property, reflections);
    setInsertionLevelProcess(property, reflections);
    setQueryLevelProcess(property, reflections);
    return property;
  }

  private static void setInsertionLevelProcess(Property property, Reflections reflections)
      throws Exception {
    Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(InsertionProcess.class);
    List<String> levelProcessClass = new ArrayList<>();
    for (Class<?> clz : annotated) {
      InsertionProcess annotationInfo = clz.getAnnotation(InsertionProcess.class);
      int level = annotationInfo.level();
      if (level < levelProcessClass.size()) {
        levelProcessClass.set(level, clz.getName());
      } else {
        for (int i = levelProcessClass.size(); i < level; i++) {
          levelProcessClass.add("");
        }
        levelProcessClass.add(clz.getName());
      }
    }
    property.setInsertionLevelProcessClass(levelProcessClass);
  }

  private static void setDeletionLevelProcess(Property property, Reflections reflections)
      throws Exception {
    Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(DeletionProcess.class);
    List<String> levelProcessClass = new ArrayList<>();
    for (Class<?> clz : annotated) {
      DeletionProcess annotationInfo = clz.getAnnotation(DeletionProcess.class);
      int level = annotationInfo.level();
      if (level < levelProcessClass.size()) {
        levelProcessClass.set(level, clz.getName());
      } else {
        for (int i = levelProcessClass.size(); i < level; i++) {
          levelProcessClass.add("");
        }
        levelProcessClass.add(clz.getName());
      }
    }
    property.setDeletionLevelProcessClass(levelProcessClass);
  }

  private static void setQueryLevelProcess(Property property, Reflections reflections)
      throws Exception {
    List<String> levelProcessClass = new ArrayList<>();
    Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(QueryProcess.class);
    for (Class<?> clz : annotated) {
      QueryProcess annotationInfo = clz.getAnnotation(QueryProcess.class);
      int level = annotationInfo.level();
      if (level < levelProcessClass.size()) {
        levelProcessClass.set(level, clz.getName());
      } else {
        for (int i = levelProcessClass.size(); i < level; i++) {
          levelProcessClass.add("");
        }
        levelProcessClass.add(clz.getName());
      }
    }
    property.setQueryLevelProcessClass(levelProcessClass);
  }
}
