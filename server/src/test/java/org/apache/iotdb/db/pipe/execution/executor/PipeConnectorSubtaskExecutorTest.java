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

package org.apache.iotdb.db.pipe.execution.executor;

import org.apache.iotdb.db.pipe.task.callable.PipeConnectorSubtask;
import org.apache.iotdb.pipe.api.PipeConnector;

import org.junit.Before;
import org.mockito.Mockito;

import java.util.concurrent.ArrayBlockingQueue;

import static org.mockito.Mockito.mock;

public class PipeConnectorSubtaskExecutorTest extends PipeSubtaskExecutorTest {

  @Before
  public void setUp() throws Exception {
    executor = new PipeConnectorSubtaskExecutor();

    subtask =
        Mockito.spy(
            new PipeConnectorSubtask(
                "PipeConnectorSubtaskExecutorTest",
                mock(PipeConnector.class),
                mock(ArrayBlockingQueue.class)) {
              @Override
              public void executeForAWhile() {}
            });
  }
}
