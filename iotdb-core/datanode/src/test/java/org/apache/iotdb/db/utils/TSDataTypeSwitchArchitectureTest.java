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

import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.Test;

public class TSDataTypeSwitchArchitectureTest {

  @Test
  public void productionCodeMustUseTypeService() {
    // Scan IoTDB production classes on this module's classpath, including its dependencies.
    // Do not freeze violations or swallow failures: existing violations must fail this test too.
    TSDataTypeSwitchRule.RULE.check(
        new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            // Avoid building the entire server dependency graph: only switch owners need checking.
            .withImportOption(location -> TSDataTypeSwitchRule.hasSwitch(location.asURI()))
            .importPackages("org.apache.iotdb"));
  }
}
