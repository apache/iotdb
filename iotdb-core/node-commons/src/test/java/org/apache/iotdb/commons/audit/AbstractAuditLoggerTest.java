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

package org.apache.iotdb.commons.audit;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.i18n.CommonMessages;

import org.apache.thrift.TException;
import org.junit.Test;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AbstractAuditLoggerTest {

  @Test
  public void testRecordTrustedChannelFailureAuditLog() {
    TestAuditLogger auditLogger = new TestAuditLogger();
    UserEntity auditEntity = new UserEntity(0, "system", "127.0.0.1");
    AtomicInteger identifierEvaluationCount = new AtomicInteger();

    auditLogger.recordTrustedChannelFailureAuditLog(
        auditEntity,
        () -> {
          identifierEvaluationCount.incrementAndGet();
          return "DataNode-1@10.0.0.1:10730";
        },
        () -> {
          identifierEvaluationCount.incrementAndGet();
          return "DataNode-2@10.0.0.2:10730";
        });

    assertSame(auditEntity, auditLogger.auditEntity);
    assertEquals(
        AuditEventType.TRUSTED_CHANNEL_FUNCTION_FAILURE,
        auditLogger.auditEntity.getAuditEventType());
    assertEquals(AuditLogOperation.CONTROL, auditLogger.auditEntity.getAuditLogOperation());
    assertFalse(auditLogger.auditEntity.getResult());
    assertEquals(0, identifierEvaluationCount.get());
    assertEquals(
        String.format(
            CommonMessages.LOG_TRUSTED_CHANNEL_FUNCTION_FAILED_INITIATOR_ARG_TARGET_ARG_E4C28443,
            "DataNode-1@10.0.0.1:10730",
            "DataNode-2@10.0.0.2:10730"),
        auditLogger.auditLog.get());
    assertEquals(2, identifierEvaluationCount.get());
  }

  @Test
  public void testIsSslFailure() {
    assertTrue(AbstractAuditLogger.isSslFailure(new SSLException("ssl failure")));
    assertTrue(
        AbstractAuditLogger.isSslFailure(
            new TException(new IOException(new SSLHandshakeException("handshake failure")))));
    assertFalse(
        AbstractAuditLogger.isSslFailure(new TException(new IOException("network failure"))));
    assertFalse(AbstractAuditLogger.isSslFailure(null));
  }

  @Test
  public void testRecordTrustedChannelFailureAuditLogIfNecessary() {
    TestAuditLogger auditLogger = new TestAuditLogger();
    TEndPoint initiator = new TEndPoint("10.0.0.1", 10730);
    TEndPoint target = new TEndPoint("10.0.0.2", 10730);

    auditLogger.recordTrustedChannelFailureAuditLogIfNecessary(
        new IOException("network failure"), initiator, target);
    assertNull(auditLogger.auditEntity);

    auditLogger.recordTrustedChannelFailureAuditLogIfNecessary(
        new SSLHandshakeException("handshake failure"), initiator, target);
    assertEquals(
        AuditEventType.TRUSTED_CHANNEL_FUNCTION_FAILURE,
        auditLogger.auditEntity.getAuditEventType());
    assertEquals(
        String.format(
            CommonMessages.LOG_TRUSTED_CHANNEL_FUNCTION_FAILED_INITIATOR_ARG_TARGET_ARG_E4C28443,
            "10.0.0.1:10730",
            "10.0.0.2:10730"),
        auditLogger.auditLog.get());
  }

  @Test
  public void testAuditFailureShouldNotReplaceTrustedChannelFailure() {
    final RuntimeException auditFailure = new RuntimeException("audit failure");
    final ThrowingAuditLogger auditLogger = new ThrowingAuditLogger(auditFailure);
    final SSLHandshakeException channelFailure = new SSLHandshakeException("handshake failure");
    final TEndPoint initiator = new TEndPoint("10.0.0.1", 10730);
    final TEndPoint target = new TEndPoint("10.0.0.2", 10730);

    auditLogger.recordTrustedChannelFailureAuditLogIfNecessary(channelFailure, initiator, target);

    assertEquals(1, auditLogger.invocationCount.get());
    assertEquals(1, channelFailure.getSuppressed().length);
    assertSame(auditFailure, channelFailure.getSuppressed()[0]);
  }

  private static class TestAuditLogger extends AbstractAuditLogger {

    private IAuditEntity auditEntity;
    private Supplier<String> auditLog;

    @Override
    public void log(IAuditEntity auditLogFields, Supplier<String> log) {
      auditEntity = auditLogFields;
      auditLog = log;
    }
  }

  private static class ThrowingAuditLogger extends AbstractAuditLogger {

    private final RuntimeException auditFailure;
    private final AtomicInteger invocationCount = new AtomicInteger();

    private ThrowingAuditLogger(final RuntimeException auditFailure) {
      this.auditFailure = auditFailure;
    }

    @Override
    public void log(final IAuditEntity auditLogFields, final Supplier<String> log) {
      invocationCount.incrementAndGet();
      throw auditFailure;
    }
  }
}
