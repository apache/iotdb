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

package org.apache.iotdb.db.pipe.receiver.visitor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.TsFileInsertionEventScanParser;
import org.apache.iotdb.db.pipe.receiver.transform.statement.PipeTreeModelConvertedInsertRowStatement;
import org.apache.iotdb.db.pipe.receiver.transform.statement.PipeTreeModelConvertedInsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This visitor transforms the data type of the statement when the statement is executed and an
 * exception occurs. The transformed statement (if any) is returned and will be executed again.
 */
public class PipeStatementTreeModelDataTypeConvertExecutionVisitor
    extends PipeStatementDataTypeConvertExecutionVisitor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeStatementTreeModelDataTypeConvertExecutionVisitor.class);

  public PipeStatementTreeModelDataTypeConvertExecutionVisitor(
      final StatementExecutor statementExecutor) {
    super(statementExecutor);
  }

  @Override
  protected Optional<TSStatus> executeConvert(final LoadTsFileStatement loadTsFileStatement) {
    for (final File file : loadTsFileStatement.getTsFiles()) {
      try (final TsFileInsertionEventScanParser container =
          new TsFileInsertionEventScanParser(
              file, new IoTDBTreePattern(null), Long.MIN_VALUE, Long.MAX_VALUE, null, null)) {
        for (final Pair<Tablet, Boolean> tabletWithIsAligned : container.toTabletWithIsAligneds()) {
          final PipeTreeModelConvertedInsertTabletStatement statement =
              new PipeTreeModelConvertedInsertTabletStatement(
                  PipeTransferTabletRawReq.toTPipeTransferRawReq(
                          tabletWithIsAligned.getLeft(), tabletWithIsAligned.getRight())
                      .constructStatement());
          TSStatus result = statementExecutor.execute(statement);

          // Retry once if the write process is rejected
          if (result.getCode() == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode()) {
            result = statementExecutor.execute(statement);
          }

          if (!(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
              || result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())) {
            return Optional.empty();
          }
        }
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to convert data type for LoadTsFileStatement: {}.", loadTsFileStatement, e);
        return Optional.empty();
      }
    }
    return Optional.of(new TSStatus());
  }

  @Override
  public Optional<TSStatus> visitInsertRow(
      final InsertRowStatement insertRowStatement, final TSStatus status) {
    return tryExecute(new PipeTreeModelConvertedInsertRowStatement(insertRowStatement));
  }

  @Override
  public Optional<TSStatus> visitInsertRows(
      final InsertRowsStatement insertRowsStatement, final TSStatus status) {
    if (insertRowsStatement.getInsertRowStatementList() == null
        || insertRowsStatement.getInsertRowStatementList().isEmpty()) {
      return Optional.empty();
    }

    final InsertRowsStatement convertedInsertRowsStatement = new InsertRowsStatement();
    convertedInsertRowsStatement.setInsertRowStatementList(
        insertRowsStatement.getInsertRowStatementList().stream()
            .map(PipeTreeModelConvertedInsertRowStatement::new)
            .collect(Collectors.toList()));
    return tryExecute(convertedInsertRowsStatement);
  }

  @Override
  public Optional<TSStatus> visitInsertRowsOfOneDevice(
      final InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, final TSStatus status) {
    if (insertRowsOfOneDeviceStatement.getInsertRowStatementList() == null
        || insertRowsOfOneDeviceStatement.getInsertRowStatementList().isEmpty()) {
      return Optional.empty();
    }

    final InsertRowsOfOneDeviceStatement convertedInsertRowsOfOneDeviceStatement =
        new InsertRowsOfOneDeviceStatement();
    convertedInsertRowsOfOneDeviceStatement.setInsertRowStatementList(
        insertRowsOfOneDeviceStatement.getInsertRowStatementList().stream()
            .map(PipeTreeModelConvertedInsertRowStatement::new)
            .collect(Collectors.toList()));
    return tryExecute(convertedInsertRowsOfOneDeviceStatement);
  }

  @Override
  public Optional<TSStatus> visitInsertTablet(
      final InsertTabletStatement insertTabletStatement, final TSStatus status) {
    return tryExecute(new PipeTreeModelConvertedInsertTabletStatement(insertTabletStatement));
  }

  @Override
  public Optional<TSStatus> visitInsertMultiTablets(
      final InsertMultiTabletsStatement insertMultiTabletsStatement, final TSStatus status) {
    if (insertMultiTabletsStatement.getInsertTabletStatementList() == null
        || insertMultiTabletsStatement.getInsertTabletStatementList().isEmpty()) {
      return Optional.empty();
    }

    final InsertMultiTabletsStatement convertedInsertMultiTabletsStatement =
        new InsertMultiTabletsStatement();
    convertedInsertMultiTabletsStatement.setInsertTabletStatementList(
        insertMultiTabletsStatement.getInsertTabletStatementList().stream()
            .map(PipeTreeModelConvertedInsertTabletStatement::new)
            .collect(Collectors.toList()));
    return tryExecute(convertedInsertMultiTabletsStatement);
  }
}
