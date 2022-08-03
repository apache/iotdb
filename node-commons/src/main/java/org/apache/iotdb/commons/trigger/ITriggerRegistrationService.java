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

package org.apache.iotdb.commons.trigger;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.trigger.exception.TriggerExecutionException;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.util.Map;

public interface ITriggerRegistrationService extends IService {

  void register(
      String triggerName,
      TriggerEvent event,
      PartialPath fullPath,
      String className,
      Map<String, String> attributes)
      throws TriggerManagementException, TriggerExecutionException;

  void deregister(String triggerName) throws TriggerManagementException;

  void activate(String triggerName) throws TriggerManagementException, TriggerExecutionException;

  void inactivate(String triggerName) throws TriggerManagementException;

  QueryDataSet show();

  @Override
  default ServiceType getID() {
    return ServiceType.TRIGGER_REGISTRATION_SERVICE;
  }
}
