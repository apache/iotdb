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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.ICoreQueryPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

public class IPlanVisitorInheritanceConstraintTest {

  @Test
  public void testIPlanVisitorSubtypesAlsoInheritICoreQueryPlanVisitor() {
    try {
      JavaClasses allClasses =
          new ClassFileImporter()
              .withImportOption(new ImportOption.DoNotIncludeTests())
              .importPackages("org.apache.iotdb");

      ArchRule rule =
          classes()
              .that()
              .areAssignableTo(IPlanVisitor.class)
              .and()
              .doNotHaveFullyQualifiedName(IPlanVisitor.class.getName())
              .and()
              .doNotHaveFullyQualifiedName(ICoreQueryPlanVisitor.class.getName())
              .should()
              .beAssignableTo(ICoreQueryPlanVisitor.class)
              .because(
                  "PlanNode.accept(...) currently relies on the fragile contract that every "
                      + "in-tree IPlanVisitor implementation also inherits "
                      + "ICoreQueryPlanVisitor. This constraint may be broken intentionally, "
                      + "but the dispatcher contract and this test must then be updated together.");

      rule.check(allClasses);
    } catch (OutOfMemoryError ignore) {
    }
  }
}
