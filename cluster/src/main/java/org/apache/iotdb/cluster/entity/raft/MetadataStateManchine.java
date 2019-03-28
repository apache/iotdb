/**
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
package org.apache.iotdb.cluster.entity.raft;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.rpc.request.ChangeMetadataRequest;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.sys.MetadataPlan;
import org.apache.iotdb.db.writelog.transfer.PhysicalPlanLogTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataStateManchine extends StateMachineAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataStateManchine.class);

  /**
   * manager of storage groups
   **/
  private MManager mManager = MManager.getInstance();

  /**
   * manager of user profile
   **/
  private IAuthorizer authorizer = LocalFileAuthorizer.getInstance();

  private AtomicLong leaderTerm = new AtomicLong(-1);

  public MetadataStateManchine() throws AuthException {
  }

  // Update StrageGroup List and userProfileMap based on Task read from raft log
  @Override
  public void onApply(Iterator iterator) {
    while (iterator.hasNext()) {

      Closure closure = null;
      ChangeMetadataRequest request = null;
      if (iterator.done() != null) {
        closure = iterator.done();
      }
      final ByteBuffer data = iterator.getData();
      try {
        request = SerializerManager.getSerializer(SerializerManager.Hessian2)
            .deserialize(data.array(), ChangeMetadataRequest.class.getName());
      } catch (final CodecException e) {
        LOGGER.error("Fail to decode IncrementAndGetRequest", e);
      }
      try {
        assert request != null;
        MetadataPlan plan = (MetadataPlan) PhysicalPlanLogTransfer
            .logToOperator(request.getPhysicalPlanBytes());
        addStorageGroup(plan.getPath().getFullPath());
      } catch (IOException | PathErrorException e) {
        LOGGER.error("Execute metadata plan error", e);
      }
      if (closure != null) {
        closure.run(Status.OK());
      }
      iterator.next();
    }
  }


  public boolean isStorageGroupLegal(String sg) {
    try {
      mManager.checkPathStorageLevelAndGetDataType(sg);
    } catch (PathErrorException e) {
      return false;
    }
    return true;
  }

  public boolean isUerProfileLegal(String username, String password) throws AuthException {
    return authorizer.login(username, password);
  }

  public void addStorageGroup(String sg) throws IOException, PathErrorException {
    mManager.setStorageLevelToMTree(sg);
  }

  public void deleteStorageGroup(String sg) {
    // TODO implement this method
  }

  public Set<String> getAllStorageGroups() throws PathErrorException {
    return mManager.getAllStorageGroup();
  }

  public void addUser(String username, String password) throws AuthException {
    authorizer.createUser(username, password);
  }

  public void deleteUSer(String username, String password) throws AuthException {
    if (isUerProfileLegal(username, password)) {
      authorizer.deleteUser(username);
    }
  }

  public void updateUser(String username, String oldPassword, String newPassword)
      throws AuthException {
    if (isUerProfileLegal(username, oldPassword)) {
      authorizer.updateUserPassword(username, newPassword);
    }
  }

  @Override
  public void onLeaderStart(final long term) {
    this.leaderTerm.set(term);
  }

  @Override
  public void onLeaderStop(final Status status) {
    this.leaderTerm.set(-1);
  }

  public boolean isLeader() {
    return this.leaderTerm.get() > 0;
  }

}
