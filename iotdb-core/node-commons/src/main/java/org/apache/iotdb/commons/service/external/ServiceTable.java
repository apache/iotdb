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

import org.apache.iotdb.commons.utils.TestOnly;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** This Class used to save the information of Services and implements methods of manipulate it. */
@NotThreadSafe
public class ServiceTable {

  private final Map<String, ServiceInformation> serviceTable;

  public ServiceTable() {
    serviceTable = new ConcurrentHashMap<>();
  }

  public ServiceTable(Map<String, ServiceInformation> serviceTable) {
    this.serviceTable = serviceTable;
  }

  // for create external service
  public void addServiceInformation(String serviceName, ServiceInformation serviceInformation) {
    serviceTable.put(serviceName, serviceInformation);
  }

  public ServiceInformation getServiceInformation(String serviceName) {
    return serviceTable.get(serviceName);
  }

  public ServiceInformation removeServiceInformation(String serviceName) {
    return serviceTable.remove(serviceName);
  }

  public boolean containsService(String serviceName) {
    return serviceTable.containsKey(serviceName);
  }

  public List<ServiceInformation> getAllServiceInformation() {
    return serviceTable.values().stream()
        .map(ServiceInformation::copy)
        .collect(Collectors.toList());
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(serviceTable.size(), outputStream);
    for (ServiceInformation serviceInformation : serviceTable.values()) {
      ReadWriteIOUtils.write(serviceInformation.serialize(), outputStream);
    }
  }

  public void deserialize(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      ServiceInformation serviceInformation = ServiceInformation.deserialize(inputStream);
      serviceTable.put(serviceInformation.getServiceName(), serviceInformation);
    }
  }

  public void clear() {
    serviceTable.clear();
  }

  @TestOnly
  public Map<String, ServiceInformation> getServiceTable() {
    return serviceTable;
  }
}
