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
package org.apache.iotdb.confignode.conf;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.Test;

import java.util.Properties;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

public class ConfigNodePropertiesTest {
  @Test
  public void TrimPropertiesOnly() {
    JavaClasses allClasses =
        new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            .importPackages("org.apache.iotdb");

    ArchRule rule =
        noClasses()
            .that()
            .areAssignableTo("org.apache.iotdb.confignode.conf.ConfigNodeDescriptor")
            .should()
            .callMethod(Properties.class, "getProperty", String.class)
            .orShould()
            .callMethod(Properties.class, "getProperty", String.class, String.class);

    rule.check(allClasses);
  }
}
