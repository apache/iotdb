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

package org.apache.iotdb.cluster.server.handlers.caller;

import org.apache.iotdb.cluster.rpc.thrift.Node;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class GetChildNodeNextLevelPathHandler implements AsyncMethodCallback<Set<String>> {

  private static final Logger logger =
      LoggerFactory.getLogger(GetChildNodeNextLevelPathHandler.class);

  private Node contact;
  private AtomicReference<Set<String>> result;

  @Override
  public void onComplete(Set<String> resp) {
    logger.info("Received child node next level path from {}", contact);
    synchronized (result) {
      result.set(resp);
      result.notifyAll();
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.warn("Cannot get child node next level path from {}, because", contact, exception);
  }

  public void setResponse(AtomicReference<Set<String>> response) {
    this.result = response;
  }

  public void setContact(Node contact) {
    this.contact = contact;
  }
}
