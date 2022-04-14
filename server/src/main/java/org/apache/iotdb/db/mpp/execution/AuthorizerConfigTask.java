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

package org.apache.iotdb.db.mpp.execution;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.AuthorNode;
import org.apache.iotdb.db.mpp.sql.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class AuthorizerConfigTask implements IConfigTask {

  private AuthorStatement authorStatement;

  public AuthorizerConfigTask() {}

  public AuthorizerConfigTask(AuthorStatement authorStatement) {
    this.authorStatement = authorStatement;
  }

  @Override
  public ListenableFuture<Void> execute() {
    ConfigIService.Client clients;
    try {
      TTransport transport = RpcTransportFactory.INSTANCE.getTransport("0.0.0.0", 22277, 2000);
      transport.open();
      clients = new ConfigIService.Client(new TBinaryProtocol(transport));
      TAuthorizerReq tAuthorizerReq =
          new TAuthorizerReq(
              authorStatement.getAuthorType().ordinal(),
              authorStatement.getUserName(),
              authorStatement.getRoleName(),
              authorStatement.getPassWord(),
              authorStatement.getNewPassword(),
              AuthorNode.strToPermissions(authorStatement.getPrivilegeList()),
              authorStatement.getNodeName().getFullPath());
      TSStatus tsStatus = clients.operatePermission(tAuthorizerReq);
      if (tsStatus.getCode() == 200) {

      }
    } catch (TTransportException | AuthException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
    return null;
  }
}
