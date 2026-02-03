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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.utils.TestOnly;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.properties.CanBeAnnotated;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.ImportOption.DoNotIncludeTests;
import com.tngtech.archunit.lang.ArchRule;
import org.apache.tsfile.annotations.MustOverride;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;

public class AnnotationTest {

  @Test
  public void checkTestOnly() {
    try {
      JavaClasses productionClasses =
          new ClassFileImporter()
              .withImportOption(new DoNotIncludeTests())
              .importPackages("org.apache.iotdb");
      JavaClasses testClasses =
          new ClassFileImporter()
              .withImportOption(new ImportOption.OnlyIncludeTests())
              .importPackages("org.apache.iotdb");

      List<Class> testReflectedClasses = new ArrayList<>();
      for (JavaClass testClass : testClasses) {
        testReflectedClasses.add(testClass.reflect());
      }

      ArchRule rule =
          methods()
              .that()
              .areAnnotatedWith(TestOnly.class)
              .should()
              .onlyBeCalled()
              .byClassesThat()
              .belongToAnyOf(testReflectedClasses.toArray(new Class[0]))
              .orShould()
              .onlyBeCalled()
              .byMethodsThat(
                  CanBeAnnotated.Predicates.annotatedWith(TestOnly.class)); // see next section

      rule.check(productionClasses);
    } catch (OutOfMemoryError ignored) {
    }
  }

  @Test
  public void checkMustOverride() {
    try {
      JavaClasses productionClasses =
          new ClassFileImporter()
              .withImportOption(new DoNotIncludeTests())
              .importPackages("org.apache.iotdb", "org.apache.tsfile");

      Map<JavaClass, List<JavaMethod>> classAndMethodsMustOverride = new HashMap<>();
      for (JavaClass productionClass : productionClasses) {
        for (JavaMethod method : productionClass.getMethods()) {
          if (method.isAnnotatedWith(MustOverride.class)) {
            classAndMethodsMustOverride
                .computeIfAbsent(productionClass, k -> new ArrayList<>())
                .add(method);
          }
        }
      }

      List<String> failures = new ArrayList<>();
      for (JavaClass productionClass : productionClasses) {
        for (Entry<JavaClass, List<JavaMethod>> entry : classAndMethodsMustOverride.entrySet()) {
          JavaClass classWithTargetMethod = entry.getKey();
          if (productionClass.isAssignableTo(classWithTargetMethod.getName())) {
            List<JavaMethod> methodsThatMustOverride = entry.getValue();
            for (JavaMethod toBeOverride : methodsThatMustOverride) {
              boolean methodImplemented = false;
              for (JavaMethod method : productionClass.getMethods()) {
                if (method.getName().equals(toBeOverride.getName())
                    && method.getParameters().equals(toBeOverride.getParameters())) {
                  methodImplemented = true;
                  break;
                }
              }
              if (!methodImplemented) {
                failures.add(
                    String.format(
                        "Class %s does not implement method %s annotated with @MustOverride from class %s",
                        productionClass.getName(),
                        toBeOverride.getName(),
                        classWithTargetMethod.getName()));
              }
            }
          }
        }
      }

      if (!failures.isEmpty()) {
        throw new AssertionError(String.join("\n", failures));
      }
    } catch (OutOfMemoryError ignored) {
    }
  }
}
