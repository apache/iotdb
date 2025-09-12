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

package org.apache.iotdb.commons.service.external;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ServiceInformation {
  private String serviceName;

  private String className;

  private boolean isUsingURI;

  private String jarName;

  /** MD5 of the Jar File */
  private String jarFileMD5;

  private ServiceStatus serviceStatus;

  public ServiceInformation() {}

  public ServiceInformation(
      String serviceName,
      String className,
      boolean isUsingURI,
      String jarName,
      String jarFileMD5,
      ServiceStatus serviceStatus) {
    this.serviceName = serviceName;
    this.className = className;
    this.isUsingURI = isUsingURI;
    this.jarName = jarName;
    this.jarFileMD5 = jarFileMD5;
    this.serviceStatus = serviceStatus;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public boolean isUsingURI() {
    return isUsingURI;
  }

  public void setUsingURI(boolean usingURI) {
    isUsingURI = usingURI;
  }

  public String getJarName() {
    return jarName;
  }

  public void setJarName(String jarName) {
    this.jarName = jarName;
  }

  public String getJarFileMD5() {
    return jarFileMD5;
  }

  public void setJarFileMD5(String jarFileMD5) {
    this.jarFileMD5 = jarFileMD5;
  }

  public ServiceStatus getServiceStatus() {
    return serviceStatus;
  }

  public void setServiceStatus(ServiceStatus serviceStatus) {
    this.serviceStatus = serviceStatus;
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(serviceName, outputStream);
    ReadWriteIOUtils.write(className, outputStream);
    ReadWriteIOUtils.write(isUsingURI, outputStream);
    ReadWriteIOUtils.write(String.valueOf(serviceStatus), outputStream);
    if (isUsingURI) {
      ReadWriteIOUtils.write(jarName, outputStream);
      ReadWriteIOUtils.write(jarFileMD5, outputStream);
    }
  }

  public static ServiceInformation deserialize(ByteBuffer byteBuffer) {
    ServiceInformation serviceInformation = new ServiceInformation();
    serviceInformation.serviceName = ReadWriteIOUtils.readString(byteBuffer);
    serviceInformation.className = ReadWriteIOUtils.readString(byteBuffer);
    serviceInformation.isUsingURI = ReadWriteIOUtils.readBool(byteBuffer);
    serviceInformation.serviceStatus =
        ServiceStatus.valueOf(ReadWriteIOUtils.readString(byteBuffer));
    if (serviceInformation.isUsingURI) {
      serviceInformation.jarName = ReadWriteIOUtils.readString(byteBuffer);
      serviceInformation.jarFileMD5 = ReadWriteIOUtils.readString(byteBuffer);
    }
    return serviceInformation;
  }

  public static ServiceInformation deserialize(InputStream inputStream) throws IOException {
    return deserialize(
        ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream)));
  }

  public ServiceInformation copy() {
    return new ServiceInformation(
        serviceName, className, isUsingURI, jarName, jarFileMD5, serviceStatus);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServiceInformation that = (ServiceInformation) o;
    return Objects.equals(serviceName, that.serviceName)
        && Objects.equals(className, that.className)
        && Objects.equals(jarName, that.jarName)
        && Objects.equals(jarFileMD5, that.jarFileMD5)
        && Objects.equals(serviceStatus, that.serviceStatus)
        && Objects.equals(isUsingURI, that.isUsingURI);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceName);
  }
}
