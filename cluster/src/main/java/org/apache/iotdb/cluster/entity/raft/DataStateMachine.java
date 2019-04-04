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
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Bits;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.callback.QPTask;
import org.apache.iotdb.cluster.callback.SingleQPTask;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.rpc.closure.ResponseClosure;
import org.apache.iotdb.cluster.rpc.request.DataGroupNonQueryRequest;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.MetadataPlan;
import org.apache.iotdb.db.writelog.transfer.PhysicalPlanLogTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StateMachine of data group node.
 */
public class DataStateMachine extends StateMachineAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataStateMachine.class);

  /**
   * Server instance
   */
  private Server server = Server.getInstance();

  /**
   * QP executor to apply task
   */
  private OverflowQPExecutor qpExecutor = new OverflowQPExecutor();

  private PeerId peerId;

  private String groupId;

  private AtomicLong leaderTerm = new AtomicLong(-1);

  private MManager mManager = MManager.getInstance();

  private final AtomicInteger requestId = new AtomicInteger(0);

  public DataStateMachine(String groupId, PeerId peerId) {
    this.peerId = peerId;
    this.groupId = groupId;
  }

  /**
   * Only deal with non query operation. The operation is completed by {@code qpExecutor}.
   *
   * @param iterator task iterator
   */
  @Override
  public void onApply(Iterator iterator) {
    while (iterator.hasNext()) {

      Closure closure = null;
      DataGroupNonQueryRequest request = null;
      BasicResponse response = null;
      final ByteBuffer data = iterator.getData();
      try {
        request = SerializerManager.getSerializer(SerializerManager.Hessian2)
            .deserialize(data.array(), DataGroupNonQueryRequest.class.getName());
      } catch (final CodecException e) {
        LOGGER.error("Fail to decode IncrementAndGetRequest", e);
      }
      if (iterator.done() != null) {
        /** It's leader to apply task **/
        closure = iterator.done();
        response = ((ResponseClosure) closure).getResponse();
      }
      Status status = Status.OK();
      assert request != null;

      List<byte[]> planBytes = request.getPhysicalPlanBytes();

      /** handle batch plans(planBytes.size() > 0) or single plan(planBytes.size()==1) **/
      for (byte[] planByte : planBytes) {
        try {
          PhysicalPlan plan = PhysicalPlanLogTransfer.logToOperator(planByte);

          /** If the request is to set path and sg of the path doesn't exist, it needs to run null-read in meta group to avoid out of data sync **/
          if (plan.getOperatorType() == OperatorType.SET_STORAGE_GROUP && !MManager.getInstance()
              .checkStorageExistOfPath(((MetadataPlan) plan).getPath().getFullPath())) {
            SingleQPTask nullReadTask = new SingleQPTask(false, null);
            try {
              LOGGER.info("Handle null-read in meta group for adding path request.");
              handleNullReadToMetaGroup(nullReadTask, status);
              nullReadTask.await();
            } catch (InterruptedException e) {
              status.setCode(-1);
              status.setErrorMsg(e.toString());
            }
          }
          qpExecutor.processNonQuery(plan);
          if (closure != null) {
            response.addResult(true);
          }
        } catch (ProcessorException | IOException e) {
          LOGGER.error("Execute physical plan error", e);
          status = new Status(-1, e.toString());
          if (closure != null) {
            response.addResult(false);
          }
        }
      }
      if (closure != null) {
        closure.run(status);
      }
      iterator.next();
    }
  }

  /**
   * Handle null-read process in metadata group if the request is to set path.
   *
   * @param qpTask null-read task
   * @param originStatus status to return result if this node is leader of the data group
   */
  private void handleNullReadToMetaGroup(QPTask qpTask, Status originStatus) {
    final byte[] reqContext = new byte[4];
    Bits.putInt(reqContext, 0, requestId.incrementAndGet());
    MetadataRaftHolder metadataRaftHolder = (MetadataRaftHolder) server.getMetadataHolder();
    ((RaftService) metadataRaftHolder.getService()).getNode()
        .readIndex(reqContext, new ReadIndexClosure() {
          @Override
          public void run(Status status, long index, byte[] reqCtx) {
            BasicResponse response = new BasicResponse(null, false, null, null) {
            };
            if (!status.isOk()) {
              originStatus.setCode(-1);
              originStatus.setErrorMsg(status.getErrorMsg());
            }
            qpTask.run(response);
          }
        });
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
