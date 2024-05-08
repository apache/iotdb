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
package org.apache.iotdb.lsm.context.applicationcontext;

import org.apache.iotdb.lsm.annotation.DeletionProcessor;
import org.apache.iotdb.lsm.annotation.InsertionProcessor;
import org.apache.iotdb.lsm.annotation.QueryProcessor;

import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Used to generate ApplicationContext object based on annotations or configuration files */
public class ApplicationContextGenerator {

  /**
   * Scan the package to get all classes, and generate ApplicationContext object based on the
   * annotations of these classes
   *
   * @param packageName package name
   * @return ApplicationContext object
   */
  public static ApplicationContext GeneratePropertyWithAnnotation(String packageName) {
    Reflections reflections =
        new Reflections(
            new ConfigurationBuilder()
                .forPackage(packageName)
                .filterInputsBy(new FilterBuilder().includePackage(packageName)));
    ApplicationContext applicationContext = new ApplicationContext();
    setDeletionLevelProcessor(applicationContext, reflections);
    setInsertionLevelProcessor(applicationContext, reflections);
    setQueryLevelProcessor(applicationContext, reflections);
    return applicationContext;
  }

  /**
   * Assign value to the insertion level processor of the ApplicationContext object
   *
   * @param applicationContext ApplicationContext object
   * @param reflections This object holds all the classes scanned in the package
   */
  private static void setInsertionLevelProcessor(
      ApplicationContext applicationContext, Reflections reflections) {
    Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(InsertionProcessor.class);
    List<String> levelProcessClass = new ArrayList<>();
    for (Class<?> clz : annotated) {
      InsertionProcessor annotationInfo = clz.getAnnotation(InsertionProcessor.class);
      setLevelProcessors(levelProcessClass, clz, annotationInfo.level());
    }
    applicationContext.setInsertionLevelProcessClass(levelProcessClass);
  }

  /**
   * Assign value to the deletion level processor of the ApplicationContext object
   *
   * @param applicationContext ApplicationContext object
   * @param reflections This object holds all the classes scanned in the package
   */
  private static void setDeletionLevelProcessor(
      ApplicationContext applicationContext, Reflections reflections) {
    Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(DeletionProcessor.class);
    List<String> levelProcessClass = new ArrayList<>();
    for (Class<?> clz : annotated) {
      DeletionProcessor annotationInfo = clz.getAnnotation(DeletionProcessor.class);
      setLevelProcessors(levelProcessClass, clz, annotationInfo.level());
    }
    applicationContext.setDeletionLevelProcessClass(levelProcessClass);
  }

  /**
   * Assign value to the query level processor of the ApplicationContext object
   *
   * @param applicationContext ApplicationContext object
   * @param reflections This object holds all the classes scanned in the package
   */
  private static <A extends Annotation> void setQueryLevelProcessor(
      ApplicationContext applicationContext, Reflections reflections) {
    List<String> levelProcessClass = new ArrayList<>();
    Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(QueryProcessor.class);
    for (Class<?> clz : annotated) {
      QueryProcessor annotationInfo = clz.getAnnotation(QueryProcessor.class);
      setLevelProcessors(levelProcessClass, clz, annotationInfo.level());
    }
    applicationContext.setQueryLevelProcessClass(levelProcessClass);
  }

  private static void setLevelProcessors(
      List<String> levelProcessorClass, Class<?> clz, int level) {
    if (level < levelProcessorClass.size()) {
      levelProcessorClass.set(level, clz.getName());
    } else {
      for (int i = levelProcessorClass.size(); i < level; i++) {
        levelProcessorClass.add("");
      }
      levelProcessorClass.add(clz.getName());
    }
  }
}
