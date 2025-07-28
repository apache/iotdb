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
import com.tngtech.archunit.core.domain.properties.CanBeAnnotated;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.ImportOption.DoNotIncludeTests;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;

public class AnnotationTest {

  @Test
  public void checkTestOnly() {
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
  }
}
