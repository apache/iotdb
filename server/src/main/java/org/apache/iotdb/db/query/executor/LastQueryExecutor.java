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

package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.factory.AggreResultFactory;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.omg.CORBA.TIMEOUT;

import java.io.IOException;
import java.sql.Time;
import java.util.*;

public class LastQueryExecutor {
    private List<Path> selectedSeries;
    private List<TSDataType> dataTypes;

    public LastQueryExecutor(LastQueryPlan lastQueryPlan) {
        this.selectedSeries = lastQueryPlan.getPaths();
        this.dataTypes = lastQueryPlan.getDataTypes();
    }

    /**
     * execute last function
     *
     * @param context query context
     */
    public QueryDataSet execute(QueryContext context)
            throws StorageEngineException, IOException, QueryProcessException {

        MNode node = null;
        try {
            node = MManager.getInstance().getNodeByPathFromCache(selectedSeries.get(0).toString()).getParent();
        } catch (PathException e) {
            throw new QueryProcessException(e);
        } catch (CacheException e) {
            throw new QueryProcessException(e.getMessage());
        }
        if (node.getCachedLastRecord() != null) {
            SingleDataSet dataSet = new SingleDataSet(selectedSeries, dataTypes);
            dataSet.setRecord(node.getCachedLastRecord());
            return dataSet;
        }

        List<TimeValuePair> lastPairList = new ArrayList<>();
        for (int i = 0; i < selectedSeries.size(); i++) {
            TimeValuePair pair = calculatLastPairForOneSeries(selectedSeries.get(i), dataTypes.get(i), context);
            lastPairList.add(pair);
        }

        RowRecord resultRecord = constructLastRowRecord(lastPairList);
        node.setCachedLastRecord(resultRecord);

        SingleDataSet dataSet = new SingleDataSet(selectedSeries, dataTypes);
        dataSet.setRecord(resultRecord);
        return dataSet;
    }

    /**
     * get aggregation result for one series
     *
     * @param context query context
     * @return AggregateResult list
     */
    private TimeValuePair calculatLastPairForOneSeries(
            Path seriesPath, TSDataType tsDataType,
            QueryContext context)
            throws IOException, QueryProcessException, StorageEngineException {

        // construct series reader without value filter
        Filter timeFilter = null;
        IAggregateReader seriesReader = new SeriesAggregateReader(
                seriesPath, tsDataType, context, QueryResourceManager.getInstance()
                .getQueryDataSource(seriesPath, context, timeFilter), timeFilter, null);

        long maxTime = Long.MIN_VALUE;
        Object value = null;

        while (seriesReader.hasNextChunk()) {
            while (seriesReader.hasNextPage()) {
                // cal by page data
                while (seriesReader.hasNextOverlappedPage()) {
                    BatchData nextOverlappedPageData = seriesReader.nextOverlappedPage();
                    int maxIndex = nextOverlappedPageData.length() - 1;
                    if (maxIndex < 0) {
                        continue;
                    }
                    long time = nextOverlappedPageData.getTimeByIndex(maxIndex);
                    if (time > maxTime) {
                        maxTime = time;
                        value = nextOverlappedPageData.getValueInTimestamp(time);
                    }
                    nextOverlappedPageData.resetBatchData();
                }
            }
        }
        return new TimeValuePair(maxTime, TsPrimitiveType.getByType(tsDataType, value));
    }

    /**
     * using last result data list construct QueryDataSet.
     *
     * @param lastPairResultList last result list
     */
    private RowRecord constructLastRowRecord(List<TimeValuePair> lastPairResultList) {
        long maxTime = Long.MIN_VALUE;
        for (TimeValuePair lastPair : lastPairResultList) {
            if (lastPair.getTimestamp() > maxTime)
                maxTime = lastPair.getTimestamp();
        }

        RowRecord resultRecord = new RowRecord(maxTime);
        for (int i = 0; i < lastPairResultList.size(); i++) {
            TSDataType dataType = dataTypes.get(i);
            TimeValuePair lastPair = lastPairResultList.get(i);
            if (lastPair.getTimestamp() == maxTime)
                resultRecord.addField(lastPair.getValue().getValue(), dataType);
            else
                resultRecord.addField(null, dataType);
        }

        return resultRecord;
    }
}
