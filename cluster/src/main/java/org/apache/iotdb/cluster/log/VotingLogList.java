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

package org.apache.iotdb.cluster.log;

import java.util.ArrayList;
import java.util.List;

public class VotingLogList {

  private List<VotingLog> logList = new ArrayList<>();
  private volatile long currTerm = -1;
  private int quorumSize;

  public VotingLogList(int quorumSize) {
    this.quorumSize = quorumSize;
  }

  /**
   * Insert a voting entry into the list. Notice the logs must be inserted in order of index, as
   * they are inserted as soon as created
   *
   * @param log
   */
  public synchronized void insert(VotingLog log) {
    if (log.getLog().getCurrLogTerm() != currTerm) {
      clear();
      currTerm = log.getLog().getCurrLogTerm();
    }
    logList.add(log);
  }

  /**
   * When an entry of index-term is strongly accepted by a node of acceptingNodeId, record the id in
   * all entries whose index <= the accepted entry. If any entry is accepted by a quorum, remove it
   * from the list.
   *
   * @param index
   * @param term
   * @param acceptingNodeId
   * @return the lastly removed entry if any.
   */
  public void onStronglyAccept(long index, long term, int acceptingNodeId) {
    int lastEntryIndexToCommit = -1;

    List<VotingLog> acceptedLogs;
    synchronized (this) {
      for (int i = 0, logListSize = logList.size(); i < logListSize; i++) {
        VotingLog votingLog = logList.get(i);
        if (votingLog.getLog().getCurrLogIndex() <= index
            && votingLog.getLog().getCurrLogTerm() == term) {
          votingLog.getStronglyAcceptedNodeIds().add(acceptingNodeId);
          if (votingLog.getStronglyAcceptedNodeIds().size() >= quorumSize) {
            lastEntryIndexToCommit = i;
          }
          if (votingLog.getStronglyAcceptedNodeIds().size()
                  + votingLog.getWeaklyAcceptedNodeIds().size()
              >= quorumSize) {
            votingLog.acceptedTime.set(System.nanoTime());
          }
        } else if (votingLog.getLog().getCurrLogIndex() > index) {
          break;
        }
      }

      List<VotingLog> tmpAcceptedLogs = logList.subList(0, lastEntryIndexToCommit + 1);
      acceptedLogs = new ArrayList<>(tmpAcceptedLogs);
      tmpAcceptedLogs.clear();
    }

    if (lastEntryIndexToCommit != -1) {
      for (VotingLog acceptedLog : acceptedLogs) {
        synchronized (acceptedLog) {
          acceptedLog.acceptedTime.set(System.nanoTime());
          acceptedLog.notifyAll();
        }
      }
    }
  }

  public synchronized void clear() {
    logList.clear();
  }

  public int size() {
    return logList.size();
  }
}
