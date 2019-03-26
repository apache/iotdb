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

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.engine.Processor;

public class DataStateMachine extends StateMachineAdapter {
  private Processor process;
  private AtomicLong leaderTerm = new AtomicLong(-1);

  public DataStateMachine() {
    //TODO init @code{process}
  }

  @Override
  public void onApply(Iterator iterator) {

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
