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

package com.timecho.iotdb.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.security.ITableAuthCheckerImpl;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DataNodeSeparationOfAdminPowersTest {

  @Test
  public void test() {
    StrictTreeAccessCheckVisitor visitor = new StrictTreeAccessCheckVisitor();
    for (AuthorType value : AuthorType.values()) {
      try {
        TSStatus status =
            visitor.visitAuthor(
                new AuthorStatement(value), new TreeAccessCheckContext(1, "user1", ""));
        if (status.getMessage().contains("Unsupported authorType")) {
          Assert.fail(
              "Unsupported authorType: "
                  + value
                  + " in StrictTreeAccessCheckVisitor.visitAuthor()");
        }
      } catch (Exception ignored) {
      }
    }
    for (AuthorRType value : AuthorRType.values()) {
      StrictAccessControlImpl accessControl =
          new StrictAccessControlImpl(new ITableAuthCheckerImpl(), visitor);
      try {
        accessControl.checkUserCanRunRelationalAuthorStatement(
            "user1", new RelationalAuthorStatement(value), new MPPQueryContext(new QueryId("1")));
      } catch (SemanticException e) {
        if (e.getMessage().contains("Unsupported authorType")) {
          Assert.fail(
              "Unsupported authorType: "
                  + value
                  + " in StrictAccessControlImpl.checkUserCanRunRelationalAuthorStatement()");
        }
      } catch (Exception ignored) {
      }
    }
  }

  /**
   * This test ensures that StrictTreeAccessCheckVisitor explicitly overrides all auth-related
   * methods from TreeAccessCheckVisitor. Any newly added permission check method in the parent
   * class must be reviewed and properly handled here to avoid privilege bypass. This test is
   * designed to fail fast during cherry-pick or version sync. example:
   * com.timecho.iotdb.auth.StrictTreeAccessCheckVisitor#checkHasGlobalAuth(org.apache.iotdb.commons.audit.IAuditEntity,
   * org.apache.iotdb.commons.auth.entity.PrivilegeType, java.util.function.Supplier, boolean)
   */
  @Test
  public void testAllCheckPermissionMethodsChecked() {
    Class<?> parent = TreeAccessCheckVisitor.class;
    Class<?> child = StrictTreeAccessCheckVisitor.class;
    Set<String> childDeclaredMethods =
        Arrays.stream(child.getDeclaredMethods())
            .map(DataNodeSeparationOfAdminPowersTest::methodSignature)
            .collect(Collectors.toSet());
    List<Method> missingReviewMethods =
        Arrays.stream(parent.getDeclaredMethods())
            .filter(DataNodeSeparationOfAdminPowersTest::mayBeCheckPrivilegeMethod)
            .filter(m -> !childDeclaredMethods.contains(methodSignature(m)))
            .filter(m -> !isMethodReviewed(m))
            .collect(Collectors.toList());

    if (!missingReviewMethods.isEmpty()) {
      String message =
          missingReviewMethods.stream().map(Method::toString).collect(Collectors.joining("\n"));

      Assert.fail(
          "StrictTreeAccessCheckVisitor must review all auth-related methods "
              + "from TreeAccessCheckVisitor.\nMissing review methods:\n"
              + message);
    }
  }

  private static boolean mayBeCheckPrivilegeMethod(Method method) {
    for (Class<?> parameterType : method.getParameterTypes()) {
      if (parameterType.equals(PrivilegeType.class)) {
        return true;
      }
    }
    return false;
  }

  private static String methodSignature(Method m) {
    return m.getName()
        + Arrays.stream(m.getParameterTypes())
            .map(Class::getName)
            .collect(Collectors.joining(",", "(", ")"));
  }

  private static boolean isMethodReviewed(Method method) {
    List<String> methodNameWhiteList =
        Arrays.asList(
            "checkOnlySuperUser",
            "checkHasGlobalAuth(org.apache.iotdb.commons.audit.IAuditEntity,org.apache.iotdb.commons.auth.entity.PrivilegeType,java.util.function.Supplier)",
            "checkTimeSeriesPermission",
            "checkTimeSeriesPermission4Pipe");
    String signature = methodSignature(method);
    for (String content : methodNameWhiteList) {
      if (signature.contains(content)) {
        return true;
      }
    }
    return false;
  }
}
