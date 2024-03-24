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

package org.apache.iotdb.session.subscription;

public class Message {

  private SubscriptionSessionDataSet dataSet;

  // TODO: support more data format

  public Message(SubscriptionSessionDataSet dataSet) {
    this.dataSet = dataSet;
  }

  public String getTopic() {
    return dataSet.getTopicName();
  }

  String getSubscriptionCommitId() {
    // make it package-private
    return dataSet.getSubscriptionCommitId();
  }

  public void debug() {
    System.out.println(dataSet.getColumnNames());
    System.out.println(dataSet.getColumnTypes());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }
  }
}
