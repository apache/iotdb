/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.service.externalservice;

import java.util.function.Supplier;

public enum BuiltinExternalServices {
  MQTT(
      "MQTT",
      "org.apache.iotdb.externalservice.Mqtt",
      // IoTDBDescriptor.getInstance().getConfig()::isEnableMQTTService
      () -> false),
  REST(
      "REST",
      "org.apache.iotdb.externalservice.Rest",
      // IoTDBRestServiceDescriptor.getInstance().getConfig()::isEnableRestService
      () -> false);

  private final String serviceName;
  private final String className;
  private final Supplier<Boolean> enabledFunction;

  BuiltinExternalServices(String serviceName, String className, Supplier<Boolean> enabledFunction) {
    this.serviceName = serviceName;
    this.className = className;
    this.enabledFunction = enabledFunction;
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getClassName() {
    return className;
  }

  public boolean isEnabled() {
    return enabledFunction.get();
  }
}
