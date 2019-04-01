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
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.PeerId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.rpc.request.MetaGroupNonQueryRequest;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.MetadataPlan;
import org.apache.iotdb.db.writelog.transfer.PhysicalPlanLogTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataStateManchine extends StateMachineAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataStateManchine.class);

  /**
   * Manager of storage groups
   **/
  private MManager mManager = MManager.getInstance();
  /**
   * Manager of user profile
   **/
  private IAuthorizer authorizer = LocalFileAuthorizer.getInstance();

  private OverflowQPExecutor qpExecutor = new OverflowQPExecutor();
  private PeerId peerId;
  private String groupId;
  private AtomicLong leaderTerm = new AtomicLong(-1);

  public MetadataStateManchine(String groupId, PeerId peerId) throws AuthException {
    this.peerId = peerId;
    this.groupId = groupId;
  }

  /**
   * Update StrageGroup List and userProfileMap based on QPTask read from raft log
   *
   * @param iterator task iterator
   */
  @Override
  public void onApply(Iterator iterator) {
    while (iterator.hasNext()) {

      Closure closure = null;
      MetaGroupNonQueryRequest request = null;
      if (iterator.done() != null) {
        closure = iterator.done();
      }
      final ByteBuffer data = iterator.getData();
      try {
        request = SerializerManager.getSerializer(SerializerManager.Hessian2)
            .deserialize(data.array(), MetaGroupNonQueryRequest.class.getName());
      } catch (final CodecException e) {
        LOGGER.error("Fail to decode IncrementAndGetRequest", e);
      }
      try {
        assert request != null;
        PhysicalPlan physicalPlan = PhysicalPlanLogTransfer
            .logToOperator(request.getPhysicalPlanBytes());
        if (physicalPlan.getOperatorType() == OperatorType.SET_STORAGE_GROUP) {
          MetadataPlan plan = (MetadataPlan) physicalPlan;
          addStorageGroup(plan.getPath().getFullPath());
        } else {
          AuthorPlan plan = (AuthorPlan) physicalPlan;
          qpExecutor.processNonQuery(plan);
        }
      } catch (IOException | PathErrorException e) {
        LOGGER.error("Execute metadata plan error", e);
      } catch (ProcessorException e) {
        LOGGER.error("Execute author plan error", e);
      }
      if (closure != null) {
        closure.run(Status.OK());
      }
      iterator.next();
    }
  }

  public void addStorageGroup(String sg) throws IOException, PathErrorException {
    mManager.setStorageLevelToMTree(sg);
  }

  public Set<String> getAllStorageGroups() throws PathErrorException {
    return mManager.getAllStorageGroup();
  }

  @Override
  public void onLeaderStart(final long term) {
    RaftUtils.updateRaftGroupLeader(groupId, peerId);
    LOGGER.info("On leader start, {} starts to be leader of {}", peerId, groupId);
    this.leaderTerm.set(term);
  }

  @Override
  public void onStartFollowing(LeaderChangeContext ctx) {
    RaftUtils.updateRaftGroupLeader(groupId, ctx.getLeaderId());
    this.leaderTerm.set(-1);
    LOGGER.info("Start following, {} starts to be leader of {}", ctx.getLeaderId(), groupId);
  }

  @Override
  public void onLeaderStop(final Status status) {
    this.leaderTerm.set(-1);
  }

  public boolean isLeader() {
    return this.leaderTerm.get() > 0;
  }

}
