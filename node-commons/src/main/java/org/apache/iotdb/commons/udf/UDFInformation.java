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

import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class UDFInformation {

  private String functionName;
  private String className;
  private boolean isBuiltin;

  private boolean isUsingURI;

  private String jarName;
  private String jarMD5;

  private UDFInformation() {}

  public UDFInformation(String functionName, String className) {
    this.functionName = functionName.toUpperCase();
    this.className = className;
  }

  public UDFInformation(
      String functionName, String className, boolean isBuiltin, boolean isUsingURI) {
    this.functionName = functionName.toUpperCase();
    this.className = className;
    this.isBuiltin = isBuiltin;
    this.isUsingURI = isUsingURI;
  }

  public UDFInformation(
      String functionName,
      String className,
      boolean isBuiltin,
      boolean isUsingURI,
      String jarName,
      String jarMD5) {
    this.functionName = functionName.toUpperCase();
    this.className = className;
    this.isBuiltin = isBuiltin;
    this.isUsingURI = isUsingURI;
    this.jarName = jarName;
    this.jarMD5 = jarMD5;
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

  public String getJarName() {
    return jarName;
  }

  public String getJarMD5() {
    return jarMD5;
  }

  public boolean isUsingURI() {
    return isUsingURI;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName.toUpperCase();
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public void setBuiltin(boolean builtin) {
    isBuiltin = builtin;
  }

  public void setJarName(String jarName) {
    this.jarName = jarName;
  }

  public void setJarMD5(String jarMD5) {
    this.jarMD5 = jarMD5;
  }

  public void setUsingURI(boolean usingURI) {
    isUsingURI = usingURI;
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(functionName, outputStream);
    ReadWriteIOUtils.write(className, outputStream);
    ReadWriteIOUtils.write(isBuiltin, outputStream);
    ReadWriteIOUtils.write(jarName, outputStream);
    ReadWriteIOUtils.write(jarMD5, outputStream);
  }

  public static UDFInformation deserialize(ByteBuffer byteBuffer) {
    UDFInformation udfInformation = new UDFInformation();
    udfInformation.setFunctionName(ReadWriteIOUtils.readString(byteBuffer));
    udfInformation.setClassName(ReadWriteIOUtils.readString(byteBuffer));
    udfInformation.setBuiltin(ReadWriteIOUtils.readBool(byteBuffer));
    udfInformation.setJarName(ReadWriteIOUtils.readString(byteBuffer));
    udfInformation.setJarMD5(ReadWriteIOUtils.readString(byteBuffer));
    return udfInformation;
  }
}
