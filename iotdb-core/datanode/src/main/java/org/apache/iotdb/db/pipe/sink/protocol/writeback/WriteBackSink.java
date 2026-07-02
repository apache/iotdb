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

package org.apache.iotdb.db.pipe.sink.protocol.writeback;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkNonReportTimeConfigurableException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MetaFormatUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MAX_DATABASE_NAME_LENGTH;

public class WriteBackSink implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteBackSink.class);
  private static final String CONNECTOR_IOTDB_DATABASE_KEY = "connector.database";
  private static final String SINK_IOTDB_DATABASE_KEY = "sink.database";

  private String targetTreeModelDatabaseName;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    validator.validateSynonymAttributes(
        Collections.singletonList(CONNECTOR_IOTDB_DATABASE_KEY),
        Collections.singletonList(SINK_IOTDB_DATABASE_KEY),
        false);

    final String targetDatabase =
        validator
            .getParameters()
            .getStringByKeys(CONNECTOR_IOTDB_DATABASE_KEY, SINK_IOTDB_DATABASE_KEY);
    if (Objects.nonNull(targetDatabase)) {
      validateTargetDatabase(targetDatabase);
    }
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    final String targetDatabase =
        parameters.getStringByKeys(CONNECTOR_IOTDB_DATABASE_KEY, SINK_IOTDB_DATABASE_KEY);
    if (Objects.nonNull(targetDatabase)) {
      targetTreeModelDatabaseName = validateTargetDatabase(targetDatabase);
    }
  }

  private static String validateTargetDatabase(final String targetDatabase) {
    final String trimmedTargetDatabase = targetDatabase.trim();
    if (trimmedTargetDatabase.startsWith(IoTDBConstant.PATH_ROOT + ".")) {
      return validateAndNormalizeTreeModelDatabaseName(trimmedTargetDatabase);
    }

    try {
      PathUtils.checkAndReturnSingleMeasurement(trimmedTargetDatabase);
      return validateAndNormalizeTreeModelDatabaseName(
          IoTDBConstant.PATH_ROOT
              + IoTDBConstant.PATH_SEPARATOR
              + trimmedTargetDatabase.toLowerCase(Locale.ENGLISH));
    } catch (final Exception e) {
      throw new PipeException(
          String.format("The target database %s is invalid.", targetDatabase), e);
    }
  }

  private static String validateAndNormalizeTreeModelDatabaseName(final String databaseName) {
    try {
      final PartialPath databasePath = new PartialPath(databaseName);
      final String[] nodes = databasePath.getNodes();
      if (nodes.length <= 1 || !IoTDBConstant.PATH_ROOT.equals(nodes[0])) {
        throw new IllegalPathException(
            databaseName, "the database name in tree model must start with 'root.'.");
      }

      final String normalizedDatabaseName = databasePath.getFullPath();
      MetaFormatUtils.checkDatabase(normalizedDatabaseName);

      if (normalizedDatabaseName.length() > MAX_DATABASE_NAME_LENGTH) {
        throw new IllegalPathException(
            normalizedDatabaseName,
            "the length of database name shall not exceed " + MAX_DATABASE_NAME_LENGTH);
      }
      return normalizedDatabaseName;
    } catch (final Exception e) {
      throw new PipeException(
          String.format(
              "The tree model database %s is invalid. The database name should match %s "
                  + "and the length should not exceed %s.",
              databaseName, IoTDBConfig.STORAGE_GROUP_PATTERN, MAX_DATABASE_NAME_LENGTH),
          e);
    }
  }

  @Override
  public void handshake() throws Exception {
    // Do nothing
  }

  @Override
  public void heartbeat() throws Exception {
    // Do nothing
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "WriteBackConnector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Ignore {}.",
          tabletInsertionEvent);
      return;
    }

    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      doTransferWrapper((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
    } else {
      doTransferWrapper((PipeRawTabletInsertionEvent) tabletInsertionEvent);
    }
  }

  @Override
  public void transfer(final Event event) throws Exception {
    if (!(event instanceof PipeHeartbeatEvent || event instanceof PipeTerminateEvent)) {
      LOGGER.warn("WriteBackConnector does not support transferring generic event: {}.", event);
    }
  }

  private void doTransferWrapper(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, WALPipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(WriteBackSink.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeInsertNodeTabletInsertionEvent);
    } finally {
      pipeInsertNodeTabletInsertionEvent.decreaseReferenceCount(
          WriteBackSink.class.getName(), false);
    }
  }

  private void doTransfer(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException {
    final TSStatus status;

    final InsertNode insertNode = pipeInsertNodeTabletInsertionEvent.getInsertNode();
    final InsertBaseStatement statement =
        PipeTransferTabletInsertNodeReq.toTPipeTransferRawReq(insertNode).constructStatement();
    rewriteTreeModelDatabaseNameIfNecessary(statement, null);
    status = statement.isEmpty() ? RpcUtils.SUCCESS_STATUS : executeStatement(statement);

    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throwWriteBackExceptionIfNecessary(
          status,
          String.format(
              "Transfer PipeInsertNodeTabletInsertionEvent %s error, result status %s",
              pipeInsertNodeTabletInsertionEvent, status));
    }
  }

  private void doTransferWrapper(final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeRawTabletInsertionEvent.increaseReferenceCount(WriteBackSink.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeRawTabletInsertionEvent);
    } finally {
      pipeRawTabletInsertionEvent.decreaseReferenceCount(WriteBackSink.class.getName(), false);
    }
  }

  private void doTransfer(final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException {
    final InsertBaseStatement statement =
        PipeTransferTabletRawReq.toTPipeTransferRawReq(
                pipeRawTabletInsertionEvent.convertToTablet(),
                pipeRawTabletInsertionEvent.isAligned())
            .constructStatement();
    rewriteTreeModelDatabaseNameIfNecessary(
        statement, pipeRawTabletInsertionEvent.getTreeModelDatabaseName());
    final TSStatus status =
        statement.isEmpty() ? RpcUtils.SUCCESS_STATUS : executeStatement(statement);

    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throwWriteBackExceptionIfNecessary(
          status,
          String.format(
              "Transfer PipeRawTabletInsertionEvent %s error, result status %s",
              pipeRawTabletInsertionEvent, status));
    }
  }

  private static void throwWriteBackExceptionIfNecessary(
      final TSStatus status, final String exceptionMessage) {
    if (status.getCode() == TSStatusCode.NO_PERMISSION.getStatusCode()) {
      throw new PipeRuntimeSinkNonReportTimeConfigurableException(exceptionMessage, Long.MAX_VALUE);
    }

    throw new PipeException(exceptionMessage);
  }

  private InsertBaseStatement rewriteTreeModelDatabaseNameIfNecessary(
      final InsertBaseStatement statement, final String sourceTreeModelDatabaseName) {
    if (Objects.isNull(targetTreeModelDatabaseName)) {
      return statement;
    }

    rewriteTreeModelDatabaseName(statement, sourceTreeModelDatabaseName);
    return statement;
  }

  private void rewriteTreeModelDatabaseName(
      final InsertBaseStatement statement, final String sourceTreeModelDatabaseName) {
    if (statement instanceof InsertRowsStatement) {
      ((InsertRowsStatement) statement)
          .getInsertRowStatementList()
          .forEach(
              rowStatement ->
                  rewriteTreeModelDatabaseName(rowStatement, sourceTreeModelDatabaseName));
      return;
    }

    if (statement instanceof InsertRowsOfOneDeviceStatement) {
      final InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement =
          (InsertRowsOfOneDeviceStatement) statement;
      insertRowsOfOneDeviceStatement
          .getInsertRowStatementList()
          .forEach(
              rowStatement ->
                  rewriteTreeModelDatabaseName(rowStatement, sourceTreeModelDatabaseName));
      insertRowsOfOneDeviceStatement.setDevicePath(
          rewriteTreeModelDevicePath(
              insertRowsOfOneDeviceStatement.getDevicePath(), sourceTreeModelDatabaseName));
      return;
    }

    if (statement instanceof InsertMultiTabletsStatement) {
      ((InsertMultiTabletsStatement) statement)
          .getInsertTabletStatementList()
          .forEach(
              tabletStatement ->
                  rewriteTreeModelDatabaseName(tabletStatement, sourceTreeModelDatabaseName));
      return;
    }

    statement.setDevicePath(
        rewriteTreeModelDevicePath(statement.getDevicePath(), sourceTreeModelDatabaseName));
  }

  private PartialPath rewriteTreeModelDevicePath(
      final PartialPath devicePath, final String sourceTreeModelDatabaseName) {
    if (Objects.isNull(devicePath)) {
      return devicePath;
    }

    final String normalizedSourceTreeModelDatabaseName =
        Objects.nonNull(sourceTreeModelDatabaseName)
            ? validateAndNormalizeTreeModelDatabaseName(sourceTreeModelDatabaseName)
            : inferTreeModelDatabaseName(devicePath);
    if (Objects.isNull(normalizedSourceTreeModelDatabaseName)) {
      return devicePath;
    }

    try {
      final String[] sourceDatabaseNodes =
          new PartialPath(normalizedSourceTreeModelDatabaseName).getNodes();
      final String[] targetDatabaseNodes = new PartialPath(targetTreeModelDatabaseName).getNodes();
      final String[] deviceNodes = devicePath.getNodes();
      if (!startsWith(deviceNodes, sourceDatabaseNodes)) {
        return devicePath;
      }

      final ArrayList<String> rebasedNodes =
          new ArrayList<>(
              targetDatabaseNodes.length + deviceNodes.length - sourceDatabaseNodes.length);
      rebasedNodes.addAll(Arrays.asList(targetDatabaseNodes));
      rebasedNodes.addAll(
          Arrays.asList(deviceNodes).subList(sourceDatabaseNodes.length, deviceNodes.length));
      return new PartialPath(rebasedNodes.toArray(new String[0]));
    } catch (final Exception e) {
      throw new PipeException(
          String.format(
              "Failed to rewrite tree model database from %s to %s for device path %s.",
              normalizedSourceTreeModelDatabaseName, targetTreeModelDatabaseName, devicePath),
          e);
    }
  }

  private String inferTreeModelDatabaseName(final PartialPath devicePath) {
    final String[] deviceNodes = devicePath.getNodes();
    if (deviceNodes.length < 2 || !IoTDBConstant.PATH_ROOT.equals(deviceNodes[0])) {
      return null;
    }

    return IoTDBConstant.PATH_ROOT + IoTDBConstant.PATH_SEPARATOR + deviceNodes[1];
  }

  private static boolean startsWith(final String[] nodes, final String[] prefixNodes) {
    if (nodes.length < prefixNodes.length) {
      return false;
    }
    for (int i = 0; i < prefixNodes.length; ++i) {
      if (!Objects.equals(nodes[i], prefixNodes[i])) {
        return false;
      }
    }
    return true;
  }

  private TSStatus executeStatement(final InsertBaseStatement statement) {
    return Coordinator.getInstance()
        .executeForTreeModel(
            new PipeEnrichedStatement(statement),
            SessionManager.getInstance().requestQueryId(),
            new SessionInfo(0, AuthorityChecker.SUPER_USER, ZoneId.systemDefault(), ""),
            "",
            ClusterPartitionFetcher.getInstance(),
            ClusterSchemaFetcher.getInstance(),
            IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold(),
            false)
        .status;
  }

  @Override
  public void close() throws Exception {
    // Do nothing
  }
}
