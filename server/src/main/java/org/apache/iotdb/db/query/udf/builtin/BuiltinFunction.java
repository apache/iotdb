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

package org.apache.iotdb.db.query.udf.builtin;

/**
 * All built-in UDFs need to register function names and full class names here.
 */
public enum BuiltinFunction {
  ;

  private final String functionName;
  private final Class<?> functionClass;
  private final String className;

  BuiltinFunction(String functionName, Class<?> functionClass) {
    this.functionName = functionName;
    this.functionClass = functionClass;
    this.className = functionClass.getName();
  }

  public String getFunctionName() {
    return functionName;
  }

  public Class<?> getFunctionClass() {
    return functionClass;
  }

  public String getClassName() {
    return className;
  }
}
