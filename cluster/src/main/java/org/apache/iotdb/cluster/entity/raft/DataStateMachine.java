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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.rpc.request.NonQueryRequest;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.transfer.PhysicalPlanLogTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStateMachine extends StateMachineAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataStateMachine.class);

  private OverflowQPExecutor qpExecutor = new OverflowQPExecutor();
  private PeerId peerId;
  private String groupId;
  private AtomicLong leaderTerm = new AtomicLong(-1);

  public DataStateMachine(String groupId, PeerId peerId) {
    this.peerId = peerId;
    this.groupId = groupId;
  }

  @Override
  public void onApply(Iterator iterator) {
    while (iterator.hasNext()) {

      Closure closure = null;
      NonQueryRequest request = null;
      if (iterator.done() != null) {
        closure = iterator.done();
      }
      final ByteBuffer data = iterator.getData();
      try {
        request = SerializerManager.getSerializer(SerializerManager.Hessian2)
            .deserialize(data.array(), NonQueryRequest.class.getName());
      } catch (final CodecException e) {
        LOGGER.error("Fail to decode IncrementAndGetRequest", e);
      }
      Status status = Status.OK();
      try {
        assert request != null;
          PhysicalPlan plan = PhysicalPlanLogTransfer
              .logToOperator(request.getPhysicalPlanBytes());
          qpExecutor.processNonQuery(plan);
      } catch (ProcessorException | IOException e) {
        LOGGER.error("Execute physical plan error", e);
        status = new Status(1, e.toString());
      }
      if (closure != null) {
        closure.run(status);
      }
      iterator.next();
    }
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
