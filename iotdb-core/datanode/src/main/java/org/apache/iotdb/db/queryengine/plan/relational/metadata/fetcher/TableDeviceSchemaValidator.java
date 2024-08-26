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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceId;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateOrUpdateDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory.truncateTailingNull;

public class TableDeviceSchemaValidator {
  private final SqlParser relationSqlParser = new SqlParser();

  private static final Logger LOGGER = LoggerFactory.getLogger(TableDeviceSchemaValidator.class);

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Coordinator coordinator = Coordinator.getInstance();

  private final TableDeviceSchemaFetcher fetcher = TableDeviceSchemaFetcher.getInstance();

  private TableDeviceSchemaValidator() {
    // do nothing
  }

  private static class TableDeviceSchemaValidatorHolder {
    private static final TableDeviceSchemaValidator INSTANCE = new TableDeviceSchemaValidator();
  }

  public static TableDeviceSchemaValidator getInstance() {
    return TableDeviceSchemaValidatorHolder.INSTANCE;
  }

  public void validateDeviceSchema(
      final ITableDeviceSchemaValidation schemaValidation, final MPPQueryContext context) {
    ValidateResult validateResult = validateDeviceSchemaInCache(schemaValidation);

    if (!validateResult.missingDeviceIndexList.isEmpty()) {
      validateResult = fetchAndValidateDeviceSchema(schemaValidation, validateResult, context);
    }

    if (!validateResult.missingDeviceIndexList.isEmpty()
        || !validateResult.attributeUpdateDeviceIndexList.isEmpty()) {
      autoCreateOrUpdateDeviceSchema(schemaValidation, validateResult, context);
    }
  }

  private ValidateResult validateDeviceSchemaInCache(
      final ITableDeviceSchemaValidation schemaValidation) {
    final ValidateResult result = new ValidateResult();
    final List<Object[]> deviceIdList = schemaValidation.getDeviceIdList();
    final List<String> attributeKeyList = schemaValidation.getAttributeColumnNameList();
    final List<Object[]> attributeValueList = schemaValidation.getAttributeValueList();

    for (int i = 0, size = deviceIdList.size(); i < size; i++) {
      final Map<String, String> attributeMap =
          fetcher
              .getTableDeviceCache()
              .getDeviceAttribute(
                  schemaValidation.getDatabase(),
                  schemaValidation.getTableName(),
                  (String[]) truncateTailingNull(deviceIdList.get(i)));
      if (attributeMap == null) {
        result.missingDeviceIndexList.add(i);
      } else {
        for (int j = 0, attributeSize = attributeKeyList.size(); j < attributeSize; j++) {
          if (!Objects.equals(
              attributeMap.get(attributeKeyList.get(j)), attributeValueList.get(i)[j])) {
            result.attributeUpdateDeviceIndexList.add(i);
            break;
          }
        }
      }
    }
    return result;
  }

  private ValidateResult fetchAndValidateDeviceSchema(
      final ITableDeviceSchemaValidation schemaValidation,
      final ValidateResult previousValidateResult,
      final MPPQueryContext context) {
    final Map<TableDeviceId, ConcurrentMap<String, String>> fetchedDeviceSchema =
        fetcher.fetchMissingDeviceSchemaForDataInsertion(
            new FetchDevice(
                schemaValidation.getDatabase(),
                schemaValidation.getTableName(),
                previousValidateResult.missingDeviceIndexList.stream()
                    .map(index -> schemaValidation.getDeviceIdList().get(index))
                    .collect(Collectors.toList())),
            context);

    fetchedDeviceSchema.forEach(
        (key, value) ->
            fetcher
                .getTableDeviceCache()
                .put(
                    schemaValidation.getDatabase(),
                    schemaValidation.getTableName(),
                    key.getIdValues(),
                    value));

    final List<String> attributeKeyList = schemaValidation.getAttributeColumnNameList();
    final List<Object[]> attributeValueList = schemaValidation.getAttributeValueList();

    final ValidateResult result = new ValidateResult();
    for (final int index : previousValidateResult.missingDeviceIndexList) {
      final Map<String, String> attributeMap =
          fetchedDeviceSchema.get(
              new TableDeviceId(
                  (String[]) truncateTailingNull(schemaValidation.getDeviceIdList().get(index))));
      if (attributeMap == null) {
        result.missingDeviceIndexList.add(index);
      } else {
        constructAttributeUpdateDeviceIndexList(
            attributeKeyList, attributeValueList, result, index, attributeMap);
      }
    }

    result.attributeUpdateDeviceIndexList.addAll(
        previousValidateResult.attributeUpdateDeviceIndexList);

    return result;
  }

  private void constructAttributeUpdateDeviceIndexList(
      final List<String> attributeKeyList,
      final List<Object[]> attributeValueList,
      final ValidateResult result,
      final int index,
      final Map<String, String> attributeMap) {
    final Object[] deviceAttributeValueList = attributeValueList.get(index);
    for (int j = 0, size = attributeKeyList.size(); j < size; j++) {
      if (deviceAttributeValueList[j] != null) {
        final String key = attributeKeyList.get(j);
        final String value = attributeMap.get(key);

        if (!deviceAttributeValueList[j].equals(value)) {
          result.attributeUpdateDeviceIndexList.add(index);
          break;
        }
      }
    }
  }

  private void autoCreateOrUpdateDeviceSchema(
      final ITableDeviceSchemaValidation schemaValidation,
      final ValidateResult previousValidateResult,
      final MPPQueryContext context) {
    final int size =
        previousValidateResult.missingDeviceIndexList.size()
            + previousValidateResult.attributeUpdateDeviceIndexList.size();
    final List<Object[]> deviceIdList = new ArrayList<>(size);
    final List<Object[]> attributeValueList = new ArrayList<>(size);

    previousValidateResult.missingDeviceIndexList.forEach(
        index -> {
          deviceIdList.add(schemaValidation.getDeviceIdList().get(index));
          attributeValueList.add(schemaValidation.getAttributeValueList().get(index));
        });

    previousValidateResult.attributeUpdateDeviceIndexList.forEach(
        index -> {
          deviceIdList.add(schemaValidation.getDeviceIdList().get(index));
          attributeValueList.add(schemaValidation.getAttributeValueList().get(index));
        });

    final ExecutionResult executionResult =
        coordinator.executeForTableModel(
            new CreateOrUpdateDevice(
                schemaValidation.getDatabase(),
                schemaValidation.getTableName(),
                deviceIdList,
                schemaValidation.getAttributeColumnNameList(),
                attributeValueList),
            relationSqlParser,
            SessionManager.getInstance().getCurrSession(),
            SessionManager.getInstance().requestQueryId(),
            SessionManager.getInstance()
                .getSessionInfo(SessionManager.getInstance().getCurrSession()),
            "Create device or update device attribute for insert",
            LocalExecutionPlanner.getInstance().metadata,
            context == null || context.getQueryType().equals(QueryType.WRITE)
                ? config.getQueryTimeoutThreshold()
                : context.getTimeOut());
    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(
          new IoTDBException(
              executionResult.status.getMessage(), executionResult.status.getCode()));
    }
  }

  private static class ValidateResult {
    final List<Integer> missingDeviceIndexList = new ArrayList<>();
    final List<Integer> attributeUpdateDeviceIndexList = new ArrayList<>();
  }
}
