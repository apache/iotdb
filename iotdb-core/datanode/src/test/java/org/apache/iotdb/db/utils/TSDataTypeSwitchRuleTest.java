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
import com.tngtech.archunit.lang.EvaluationResult;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.junit.Test;

import java.util.List;
import java.util.function.ToIntFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TSDataTypeSwitchRuleTest {

  @Test
  public void detectsStatementAndExpression() {
    // Both javac switch forms must report the business method and a nonzero source line.
    List<String> violations = violations(Switches.class);
    assertEquals(2, violations.size());
    assertTrue(violations.stream().anyMatch(message -> message.contains(".statement(")));
    assertTrue(violations.stream().anyMatch(message -> message.contains(".expression(")));
    for (String message : violations) {
      assertTrue(message, message.matches(".*TSDataTypeSwitchRuleTest.java:[1-9][0-9]*.*"));
    }
  }

  @Test
  public void detectsLambdaAndAnonymousClass() {
    // Switches cannot escape detection by being moved into lambdas or anonymous classes.
    assertEquals(1, violations(LambdaSwitch.class).size());
    assertEquals(1, violations(AnonymousSwitch.VALUE.getClass()).size());
  }

  @Test
  public void allowsOtherEnumsAndOrdinal() {
    // An ordinal read beside an unrelated enum switch is legal, as is reading an enum constant.
    assertTrue(violations(OrdinaryUsage.class).isEmpty());
  }

  @Test
  public void allowsTypeServiceImplementationAndRegisteredHolder() {
    // Actual TypeService implementations and the existing holder are valid dispatch locations.
    assertTrue(violations(SwitchService.class, TypeServices.class).isEmpty());
  }

  @Test
  public void doesNotExemptSimilarlyNamedBusinessClass() {
    // A class name containing TypeService is not sufficient to bypass the rule.
    assertFalse(violations(NotATypeService.class).isEmpty());
  }

  private static List<String> violations(Class<?>... classes) {
    EvaluationResult result =
        TSDataTypeSwitchRule.RULE.evaluate(new ClassFileImporter().importClasses(classes));
    return result.getFailureReport().getDetails();
  }

  @Test
  public void onlyExemptsServiceLambdaInsideHolder() {
    List<String> violations = violations(MixedHolder.class);
    assertEquals(2, violations.size());
    assertTrue(violations.stream().anyMatch(message -> message.contains(".business(")));
  }

  static class Switches {
    int statement(TSDataType type) {
      switch (type) {
        case INT32:
          return 1;
        default:
          return 0;
      }
    }

    int expression(TSDataType type) {
      return switch (type) {
        case DOUBLE -> 2;
        default -> 0;
      };
    }
  }

  static class LambdaSwitch {
    ToIntFunction<TSDataType> value() {
      return type ->
          switch (type) {
            case FLOAT -> 3;
            default -> 0;
          };
    }
  }

  static class AnonymousSwitch {
    static final ToIntFunction<TSDataType> VALUE =
        new ToIntFunction<TSDataType>() {
          @Override
          public int applyAsInt(TSDataType type) {
            return switch (type) {
              case INT64 -> 4;
              default -> 0;
            };
          }
        };
  }

  static class OrdinaryUsage {
    int value(TSDataType type, Thread.State state) {
      int ordinal = type.ordinal();
      int stateValue =
          switch (state) {
            case NEW -> 1;
            default -> 0;
          };
      return ordinal + stateValue + TSDataType.INT32.ordinal();
    }
  }

  static class SwitchService implements TypeService<Integer> {
    @Override
    public Integer call(Type type) {
      return dispatch(TSDataType.INT32);
    }

    int dispatch(TSDataType type) {
      return switch (type) {
        case INT32 -> 1;
        default -> 0;
      };
    }
  }

  static class NotATypeService {
    int dispatch(TSDataType type) {
      return switch (type) {
        case INT32 -> 1;
        default -> 0;
      };
    }
  }

  static class MixedHolder {
    static final TypeService<Integer> SERVICE =
        type ->
            switch (TSDataType.INT32) {
              case INT32 -> 1;
              default -> 0;
            };

    static final ToIntFunction<TSDataType> ORDINARY =
        type ->
            switch (type) {
              case INT32 -> 1;
              default -> 0;
            };

    int business(TSDataType type) {
      return switch (type) {
        case INT32 -> 1;
        default -> 0;
      };
    }
  }
}
