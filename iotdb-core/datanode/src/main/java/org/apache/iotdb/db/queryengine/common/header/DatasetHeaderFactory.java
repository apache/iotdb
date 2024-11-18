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

package org.apache.iotdb.db.queryengine.common.header;

public class DatasetHeaderFactory {

  private DatasetHeaderFactory() {
    // forbidding instantiation
  }

  public static DatasetHeader getCountStorageGroupHeader() {
    return new DatasetHeader(ColumnHeaderConstant.countStorageGroupColumnHeaders, true);
  }

  public static DatasetHeader getCountNodesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.countNodesColumnHeaders, true);
  }

  public static DatasetHeader getCountDevicesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.countDevicesColumnHeaders, true);
  }

  public static DatasetHeader getCountTimeSeriesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.countTimeSeriesColumnHeaders, true);
  }

  public static DatasetHeader getCountLevelTimeSeriesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.countLevelTimeSeriesColumnHeaders, true);
  }

  public static DatasetHeader getShowTimeSeriesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showTimeSeriesColumnHeaders, true);
  }

  public static DatasetHeader getShowDevicesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showDevicesColumnHeaders, true);
  }

  public static DatasetHeader getShowDevicesWithSgHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showDevicesWithSgColumnHeaders, true);
  }

  public static DatasetHeader getShowStorageGroupHeader(final boolean isDetailed) {
    return isDetailed
        ? new DatasetHeader(ColumnHeaderConstant.showStorageGroupsDetailColumnHeaders, true)
        : new DatasetHeader(ColumnHeaderConstant.showStorageGroupsColumnHeaders, true);
  }

  public static DatasetHeader getShowTTLHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showTTLColumnHeaders, true);
  }

  public static DatasetHeader getShowChildPathsHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showChildPathsColumnHeaders, true);
  }

  public static DatasetHeader getShowChildNodesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showChildNodesColumnHeaders, true);
  }

  public static DatasetHeader getShowVersionHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showVersionColumnHeaders, true);
  }

  public static DatasetHeader getLastQueryHeader() {
    return new DatasetHeader(ColumnHeaderConstant.lastQueryColumnHeaders, false);
  }

  public static DatasetHeader getShowClusterHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showClusterColumnHeaders, true);
  }

  public static DatasetHeader getShowClusterParametersHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showVariablesColumnHeaders, true);
  }

  public static DatasetHeader getShowClusterDetailsHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showClusterDetailsColumnHeaders, true);
  }

  public static DatasetHeader getShowClusterIdHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showClusterIdColumnHeaders, true);
  }

  public static DatasetHeader getTestConnectionHeader() {
    return new DatasetHeader(ColumnHeaderConstant.testConnectionColumnHeaders, true);
  }

  public static DatasetHeader getShowFunctionsHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showFunctionsColumnHeaders, true);
  }

  public static DatasetHeader getShowTriggersHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showTriggersColumnHeaders, true);
  }

  public static DatasetHeader getShowPipePluginsHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showPipePluginsColumnHeaders, true);
  }

  public static DatasetHeader getShowRegionHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showRegionColumnHeaders, true);
  }

  public static DatasetHeader getShowAINodesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showAINodesColumnHeaders, true);
  }

  public static DatasetHeader getShowDataNodesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showDataNodesColumnHeaders, true);
  }

  public static DatasetHeader getShowConfigNodesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showConfigNodesColumnHeaders, true);
  }

  public static DatasetHeader getShowSchemaTemplateHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showSchemaTemplateHeaders, true);
  }

  public static DatasetHeader getShowNodesInSchemaTemplateHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showNodesInSchemaTemplateHeaders, true);
  }

  public static DatasetHeader getShowPathSetTemplateHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showPathSetTemplateHeaders, true);
  }

  public static DatasetHeader getShowPathsUsingTemplateHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showPathsUsingTemplateHeaders, true);
  }

  public static DatasetHeader getShowPipeSinkTypeHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showPipeSinkTypeColumnHeaders, true);
  }

  public static DatasetHeader getShowPipeSinkHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showPipeSinkColumnHeaders, true);
  }

  public static DatasetHeader getShowPipeHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showPipeColumnHeaders, true);
  }

  public static DatasetHeader getShowTopicHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showTopicColumnHeaders, true);
  }

  public static DatasetHeader getShowSubscriptionHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showSubscriptionColumnHeaders, true);
  }

  public static DatasetHeader getGetRegionIdHeader() {
    return new DatasetHeader(ColumnHeaderConstant.getRegionIdColumnHeaders, true);
  }

  public static DatasetHeader getGetSeriesSlotListHeader() {
    return new DatasetHeader(ColumnHeaderConstant.getSeriesSlotListColumnHeaders, true);
  }

  public static DatasetHeader getGetTimeSlotListHeader() {
    return new DatasetHeader(ColumnHeaderConstant.getTimeSlotListColumnHeaders, true);
  }

  public static DatasetHeader getCountTimeSlotListHeader() {
    return new DatasetHeader(ColumnHeaderConstant.countTimeSlotListColumnHeaders, true);
  }

  public static DatasetHeader getSelectIntoHeader(boolean isAlignByDevice) {
    return isAlignByDevice
        ? new DatasetHeader(ColumnHeaderConstant.selectIntoAlignByDeviceColumnHeaders, true)
        : new DatasetHeader(ColumnHeaderConstant.selectIntoColumnHeaders, true);
  }

  public static DatasetHeader getShowContinuousQueriesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showContinuousQueriesColumnHeaders, true);
  }

  public static DatasetHeader getShowQueriesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showQueriesColumnHeaders, false);
  }

  public static DatasetHeader getShowSpaceQuotaHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showSpaceQuotaColumnHeaders, true);
  }

  public static DatasetHeader getShowThrottleQuotaHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showThrottleQuotaColumnHeaders, true);
  }

  public static DatasetHeader getShowModelsHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showModelsColumnHeaders, true);
  }

  public static DatasetHeader getShowLogicalViewHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showLogicalViewColumnHeaders, true);
  }

  public static DatasetHeader getShowCurrentTimestampHeader() {
    return new DatasetHeader(ColumnHeaderConstant.SHOW_CURRENT_TIMESTAMP_COLUMN_HEADERS, true);
  }

  public static DatasetHeader getShowDBHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showDBColumnHeaders, true);
  }

  public static DatasetHeader getShowDBDetailsHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showDBDetailsColumnHeaders, true);
  }

  public static DatasetHeader getDescribeTableHeader() {
    return new DatasetHeader(ColumnHeaderConstant.describeTableColumnHeaders, true);
  }

  public static DatasetHeader getDescribeTableDetailsHeader() {
    return new DatasetHeader(ColumnHeaderConstant.describeTableDetailsColumnHeaders, true);
  }

  public static DatasetHeader getShowTablesHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showTablesColumnHeaders, true);
  }

  public static DatasetHeader getShowTablesDetailsHeader() {
    return new DatasetHeader(ColumnHeaderConstant.showTablesDetailsColumnHeaders, true);
  }

  public static DatasetHeader getShowCurrentUserHeader() {
    return new DatasetHeader(ColumnHeaderConstant.SHOW_CURRENT_USER_COLUMN_HEADERS, true);
  }

  public static DatasetHeader getShowCurrentDatabaseHeader() {
    return new DatasetHeader(ColumnHeaderConstant.SHOW_CURRENT_DATABASE_COLUMN_HEADERS, true);
  }

  public static DatasetHeader getShowCurrentSqlDialectHeader() {
    return new DatasetHeader(ColumnHeaderConstant.SHOW_CURRENT_SQL_DIALECT_COLUMN_HEADERS, true);
  }
}
