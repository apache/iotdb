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

package org.apache.iotdb.db.pipe.agent.task;

import org.apache.iotdb.commons.pipe.agent.task.connection.EventSupplier;
import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.pipe.api.PipeProcessor;

import org.junit.Before;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

public class PipeProcessorSubtaskExecutorTest extends PipeSubtaskExecutorTest {

  @Before
  public void setUp() throws Exception {
    executor = new PipeProcessorSubtaskExecutor();

    subtask =
        Mockito.spy(
            new PipeProcessorSubtask(
                "PipeProcessorSubtaskExecutorTest",
                "TestPipe",
                System.currentTimeMillis(),
                0,
                mock(EventSupplier.class),
                mock(PipeProcessor.class),
                mock(PipeEventCollector.class)));
  }
}
