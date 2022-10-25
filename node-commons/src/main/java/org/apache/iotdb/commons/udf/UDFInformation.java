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

package org.apache.iotdb.commons.udf;

import java.nio.ByteBuffer;

public class UDFInformation {

  private final String functionName;
  private final String className;
  private final boolean isBuiltin;
  private final String jarName;
  private final String jarMD5;

  public UDFInformation(String functionName, String className) {
    this.functionName = functionName.toUpperCase();
    this.className = className;
    isBuiltin = true;
    jarName = null;
    jarMD5 = null;
  }

  public UDFInformation(
      String functionName, String className, boolean isBuiltin, String jarName, String jarMD5) {
    this.functionName = functionName.toUpperCase();
    this.className = className;
    this.isBuiltin = isBuiltin;
    this.jarName = jarName;
    this.jarMD5 = jarMD5;
  }

  public static UDFInformation deserialize(ByteBuffer byteBuffer) {
    // TODO
    return null;
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getClassName() {
    return className;
  }

  public boolean isBuiltin() {
    return isBuiltin;
  }

  public boolean isUDTF() {
    return true;
  }

  public boolean isUDAF() {
    return false;
  }
}
