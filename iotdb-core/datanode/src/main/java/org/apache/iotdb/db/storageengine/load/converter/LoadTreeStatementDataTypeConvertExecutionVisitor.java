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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.TsFileInsertionEventScanParser;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Optional;

public class LoadTreeStatementDataTypeConvertExecutionVisitor
    extends StatementVisitor<Optional<TSStatus>, Void> {

  private final StatementExecutor statementExecutor;

  private static final Logger LOGGER =
      LoggerFactory.getLogger(LoadTreeStatementDataTypeConvertExecutionVisitor.class);

  @FunctionalInterface
  public interface StatementExecutor {
    TSStatus execute(final Statement statement);
  }

  public LoadTreeStatementDataTypeConvertExecutionVisitor(
      final StatementExecutor statementExecutor) {
    this.statementExecutor = statementExecutor;
  }

  @Override
  public Optional<TSStatus> visitNode(final StatementNode statementNode, final Void v) {
    return Optional.empty();
  }

  @Override
  public Optional<TSStatus> visitLoadFile(
      final LoadTsFileStatement loadTsFileStatement, final Void v) {

    LOGGER.info("Start data type conversion for LoadTsFileStatement: {}", loadTsFileStatement);

    for (final File file : loadTsFileStatement.getTsFiles()) {
      try (final TsFileInsertionEventScanParser parser =
          new TsFileInsertionEventScanParser(
              file, new IoTDBTreePattern(null), Long.MIN_VALUE, Long.MAX_VALUE, null, null)) {
        for (final Pair<Tablet, Boolean> tabletWithIsAligned : parser.toTabletWithIsAligneds()) {
          final LoadConvertedInsertTabletStatement statement =
              new LoadConvertedInsertTabletStatement(
                  PipeTransferTabletRawReq.toTPipeTransferRawReq(
                          tabletWithIsAligned.getLeft(), tabletWithIsAligned.getRight())
                      .constructStatement(),
                  loadTsFileStatement.isConvertOnTypeMismatch());

          TSStatus result;
          try {
            result =
                statement.accept(
                    LoadTsFileDataTypeConverter.STATEMENT_STATUS_VISITOR,
                    statementExecutor.execute(statement));

            // Retry max 5 times if the write process is rejected
            for (int i = 0;
                i < 5
                    && result.getCode()
                        == TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode();
                i++) {
              Thread.sleep(100L * (i + 1));
              result =
                  statement.accept(
                      LoadTsFileDataTypeConverter.STATEMENT_STATUS_VISITOR,
                      statementExecutor.execute(statement));
            }
          } catch (final Exception e) {
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
            result = statement.accept(LoadTsFileDataTypeConverter.STATEMENT_EXCEPTION_VISITOR, e);
          }

          if (!(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
              || result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
              || result.getCode()
                  == TSStatusCode.LOAD_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())) {
            LOGGER.warn(
                "Failed to convert data type for LoadTsFileStatement: {}, status code is {}.",
                loadTsFileStatement,
                result.getCode());
            return Optional.empty();
          }
        }
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to convert data type for LoadTsFileStatement: {}.", loadTsFileStatement, e);
        return Optional.empty();
      }
    }

    if (loadTsFileStatement.isDeleteAfterLoad()) {
      loadTsFileStatement
          .getTsFiles()
          .forEach(
              tsfile -> {
                FileUtils.deleteQuietly(tsfile);
                final String tsFilePath = tsfile.getAbsolutePath();
                FileUtils.deleteQuietly(new File(tsFilePath + TsFileResource.RESOURCE_SUFFIX));
                FileUtils.deleteQuietly(new File(tsFilePath + ModificationFileV1.FILE_SUFFIX));
                FileUtils.deleteQuietly(new File(tsFilePath + ModificationFile.FILE_SUFFIX));
              });
    }

    LOGGER.info(
        "Data type conversion for LoadTsFileStatement {} is successful.", loadTsFileStatement);

    return Optional.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }
}
