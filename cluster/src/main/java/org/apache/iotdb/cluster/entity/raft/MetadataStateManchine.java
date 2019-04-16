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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.rpc.raft.closure.ResponseClosure;
import org.apache.iotdb.cluster.rpc.raft.request.nonquery.MetaGroupNonQueryRequest;
import org.apache.iotdb.cluster.rpc.raft.response.BasicResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
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

/**
 * StateMachine of metadata group node.
 */
public class MetadataStateManchine extends StateMachineAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataStateManchine.class);

  /**
   * Manager of storage groups
   **/
  private MManager mManager = MManager.getInstance();

  private OverflowQPExecutor qpExecutor = new OverflowQPExecutor();

  private PeerId peerId;

  private String groupId;

  private AtomicLong leaderTerm = new AtomicLong(-1);

  public MetadataStateManchine(String groupId, PeerId peerId) {
    this.peerId = peerId;
    this.groupId = groupId;
  }

  /**
   * Update StrageGroup List and userProfileMap based on QPTask read from raft log.
   * SG by {@code mManager}, User Info by {@code qpExecutor}
   *
   * @param iterator task iterator
   */
  @Override
  public void onApply(Iterator iterator) {
    while (iterator.hasNext()) {
      final ByteBuffer data = iterator.getData();
      final Closure closure = iterator.done();
      applySingleTask(closure, data);
      iterator.next();
    }
  }

  private void applySingleTask(Closure closure, ByteBuffer data){
    /** If closure is not null, the node is leader **/
    BasicResponse response = (closure==null) ? null: ((ResponseClosure)closure).getResponse();
    MetaGroupNonQueryRequest request;

    try {
      request = SerializerManager.getSerializer(SerializerManager.Hessian2)
          .deserialize(data.array(), MetaGroupNonQueryRequest.class.getName());
    } catch (final CodecException e) {
      LOGGER.error("Fail to deserialize MetadataGroupNonQueryRequest", e);
      if(closure != null){
        closure.run(RaftUtils.createErrorStatus(e.getMessage()));
      }
      return;
    }
    Status status = new Status();

    List<byte[]> planBytes = request.getPhysicalPlanBytes();

    /** handle batch plans(planBytes.size() > 0) or single plan(planBytes.size()==1) **/
    for (byte[] planByte : planBytes) {
      try {
        PhysicalPlan physicalPlan = PhysicalPlanLogTransfer
            .logToOperator(planByte);
        if (physicalPlan.getOperatorType() == OperatorType.SET_STORAGE_GROUP) {
          MetadataPlan plan = (MetadataPlan) physicalPlan;
          addStorageGroup(plan.getPath().getFullPath());
        } else {
          AuthorPlan plan = (AuthorPlan) physicalPlan;
          qpExecutor.processNonQuery(plan);
        }
        addResult(response, true);
      } catch (IOException | PathErrorException e) {
        LOGGER.error("Execute metadata plan error", e);
        status = new Status(-1, e.getMessage());
        addResult(response, false);
      } catch (ProcessorException e) {
        LOGGER.error("Execute author plan error", e);
        status = new Status(-1, e.getMessage());
        addResult(response, false);
      }
    }
    if (closure != null) {
      closure.run(status);
    }
  }

  /**
   * Add result to response
   */
  private void addResult(BasicResponse response, boolean result){
    if(response != null){
      response.addResult(result);
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
