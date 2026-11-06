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

package org.apache.iotdb.commons.pipe.agent.task.progress;

import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@RunWith(Parameterized.class)
public class PipeEventCommitManagerTest {

  private static final String PIPE_NAME = "late-commit-test";
  private static final long CREATION_TIME = 1L;
  private static final int REGION_ID = 1;

  private final Level logLevel;
  private final Logger logger = (Logger) LoggerFactory.getLogger(PipeEventCommitManager.class);
  private Level originalLogLevel;
  private PipeEventCommitManager manager;

  public PipeEventCommitManagerTest(final Level logLevel) {
    this.logLevel = logLevel;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Level> logLevels() {
    return Arrays.asList(Level.INFO, Level.DEBUG);
  }

  @Before
  public void setUp() throws Exception {
    originalLogLevel = logger.getLevel();
    logger.setLevel(logLevel);
    final Constructor<PipeEventCommitManager> constructor =
        PipeEventCommitManager.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    manager = constructor.newInstance();

    final PipeTaskAgent taskAgent = mock(PipeTaskAgent.class);
    when(taskAgent.getCommitterKey(eq(PIPE_NAME), eq(CREATION_TIME), eq(REGION_ID), anyInt()))
        .thenAnswer(
            invocation ->
                new CommitterKey(PIPE_NAME, CREATION_TIME, REGION_ID, invocation.getArgument(3)));
    manager.setTaskAgent(taskAgent);
    manager.register(PIPE_NAME, CREATION_TIME, REGION_ID, "iotdb-thrift-sink");
  }

  @After
  public void tearDown() {
    try {
      if (manager != null) {
        manager.clear(PIPE_NAME, CREATION_TIME);
      }
    } finally {
      logger.setLevel(originalLogLevel);
    }
  }

  @Test
  public void testLateSingleCommitAfterClearDoesNotRestoreState() throws Exception {
    final CommitterKey key = manager.getCommitterKey(PIPE_NAME, CREATION_TIME, REGION_ID);
    final Runnable onCommitted = mock(Runnable.class);
    final EnrichedEvent event = createEvent(onCommitted);
    manager.clear(PIPE_NAME, CREATION_TIME);

    manager.commit(event, key);

    assertTrue(getManagerMap("eventCommitterMap").isEmpty());
    assertTrue(getManagerMap("eventCommitterRestartTimesMap").isEmpty());
    verify(onCommitted, never()).run();
  }

  @Test
  public void testLateMultipleCommitsAfterClearDoNotRestoreState() throws Exception {
    final CommitterKey key = manager.getCommitterKey(PIPE_NAME, CREATION_TIME, REGION_ID);
    final Runnable onCommitted = mock(Runnable.class);
    final EnrichedEvent event = createEvent(onCommitted);
    when(event.hasMultipleCommitIds()).thenReturn(true);
    when(event.getCommitIds()).thenReturn(Arrays.asList(1L, 2L, 3L));
    manager.clear(PIPE_NAME, CREATION_TIME);

    manager.commit(event, key);

    assertTrue(getManagerMap("eventCommitterMap").isEmpty());
    assertTrue(getManagerMap("eventCommitterRestartTimesMap").isEmpty());
    verify(onCommitted, never()).run();
  }

  @Test
  public void testRestartRejectsStaleCommitAndAcceptsCurrentCommit() throws Exception {
    final CommitterKey oldKey = manager.getCommitterKey(PIPE_NAME, CREATION_TIME, REGION_ID);
    final Runnable oldOnCommitted = mock(Runnable.class);
    manager.deregister(PIPE_NAME, CREATION_TIME, REGION_ID);

    manager.commit(createEvent(oldOnCommitted), oldKey);
    assertEquals(
        Integer.valueOf(1),
        getManagerMap("eventCommitterRestartTimesMap")
            .get(new CommitterKey(PIPE_NAME, CREATION_TIME, REGION_ID)));

    manager.register(PIPE_NAME, CREATION_TIME, REGION_ID, "iotdb-thrift-sink");
    final CommitterKey newKey = manager.getCommitterKey(PIPE_NAME, CREATION_TIME, REGION_ID);
    assertEquals(1, newKey.getRestartTimes());
    final Runnable newOnCommitted = mock(Runnable.class);
    manager.commit(createEvent(oldOnCommitted), oldKey);
    manager.commit(createEvent(newOnCommitted), newKey);

    verify(oldOnCommitted, never()).run();
    verify(newOnCommitted).run();
    assertEquals(1, getManagerMap("eventCommitterMap").size());
    assertEquals(1, getManagerMap("eventCommitterRestartTimesMap").size());
  }

  private static EnrichedEvent createEvent(final Runnable onCommitted) {
    final EnrichedEvent event =
        mock(
            EnrichedEvent.class,
            withSettings()
                .useConstructor(
                    PIPE_NAME,
                    CREATION_TIME,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false,
                    Long.MIN_VALUE,
                    Long.MAX_VALUE));
    when(event.needToCommit()).thenReturn(true);
    when(event.getCommitId()).thenReturn(1L);
    when(event.getOnCommittedHooks()).thenReturn(Collections.singletonList(onCommitted));
    return event;
  }

  private Map<?, ?> getManagerMap(final String fieldName) throws Exception {
    final Field field = PipeEventCommitManager.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (Map<?, ?>) field.get(manager);
  }
}
