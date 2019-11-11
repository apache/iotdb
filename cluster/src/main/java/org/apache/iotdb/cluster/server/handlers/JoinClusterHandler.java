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

package org.apache.iotdb.cluster.server.handlers;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient.addNode_call;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinClusterHandler implements AsyncMethodCallback<addNode_call> {

  private static final Logger logger = LoggerFactory.getLogger(JoinClusterHandler.class);

  private Node contact;
  private AtomicLong response;

  @Override
  public void onComplete(addNode_call call) {
    try {
      long resp = call.getResult();
      logger.debug("Received a join cluster response {} from {}", resp, contact);
      synchronized (response) {
        response.set(resp);
        response.notifyAll();
      }
    } catch (TException e) {
      onError(e);
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.warn("Cannot join the cluster from {}, because", contact, exception);
    synchronized (response) {
      response.set(RaftServer.RESPONSE_REJECT);
      response.notifyAll();
    }
  }

  public void setResponse(AtomicLong response) {
    this.response = response;
  }

  public void setContact(Node contact) {
    this.contact = contact;
  }
}
