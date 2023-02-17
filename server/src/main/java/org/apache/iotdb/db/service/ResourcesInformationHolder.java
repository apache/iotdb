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

package org.apache.iotdb.db.service;

import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.udf.UDFInformation;

import java.util.List;

public class ResourcesInformationHolder {
  private static final int JAR_NUM_OF_ONE_RPC = 10;

  /** store the list when registering in config node for preparing udf related resources */
  private List<UDFInformation> udfInformationList;

  /** store the list when registering in config node for preparing trigger related resources */
  private List<TriggerInformation> triggerInformationList;

  public static int getJarNumOfOneRpc() {
    return JAR_NUM_OF_ONE_RPC;
  }

  public List<UDFInformation> getUDFInformationList() {
    return udfInformationList;
  }

  public void setUDFInformationList(List<UDFInformation> udfInformationList) {
    this.udfInformationList = udfInformationList;
  }

  public List<TriggerInformation> getTriggerInformationList() {
    return triggerInformationList;
  }

  public void setTriggerInformationList(List<TriggerInformation> triggerInformationList) {
    this.triggerInformationList = triggerInformationList;
  }
}
