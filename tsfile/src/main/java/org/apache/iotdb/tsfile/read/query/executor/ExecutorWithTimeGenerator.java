/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.read.query.executor;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.DataSetWithTimeGenerator;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGeneratorImpl;
import org.apache.iotdb.tsfile.read.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGeneratorImpl;
import org.apache.iotdb.tsfile.read.reader.series.SeriesReaderByTimestamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class ExecutorWithTimeGenerator implements QueryExecutor {

    private MetadataQuerier metadataQuerier;
    private ChunkLoader chunkLoader;

    public ExecutorWithTimeGenerator(MetadataQuerier metadataQuerier, ChunkLoader chunkLoader) {
        this.metadataQuerier = metadataQuerier;
        this.chunkLoader = chunkLoader;
    }

    /**
     * All leaf nodes of queryFilter in queryExpression are SeriesFilters, We use a TimeGenerator to control query
     * processing.
     *
     * for more information, see DataSetWithTimeGenerator
     *
     * @return DataSet with TimeGenerator
     */
    @Override
    public DataSetWithTimeGenerator execute(QueryExpression queryExpression) throws IOException {

        IExpression IExpression = queryExpression.getExpression();
        List<Path> selectedPathList = queryExpression.getSelectedSeries();

        // get TimeGenerator by IExpression
        TimeGenerator timeGenerator = new TimeGeneratorImpl(IExpression, chunkLoader, metadataQuerier);

        // the size of hasFilter is equal to selectedPathList, if a series has a filter, it is true, otherwise false
        List<Boolean> cached = removeFilteredPaths(IExpression, selectedPathList);
        List<SeriesReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();

        for (int i = 0; i < cached.size(); i++) {

            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(selectedPathList.get(i));
            dataTypes.add(chunkMetaDataList.get(0).getTsDataType());

            if (cached.get(i)) {
                readersOfSelectedSeries.add(null);
                continue;
            }

            SeriesReaderByTimestamp seriesReader = new SeriesReaderByTimestamp(chunkLoader, chunkMetaDataList);
            readersOfSelectedSeries.add(seriesReader);
        }

        return new DataSetWithTimeGenerator(selectedPathList, cached, dataTypes, timeGenerator,
                readersOfSelectedSeries);
    }

    private List<Boolean> removeFilteredPaths(IExpression IExpression, List<Path> selectedPaths) {

        List<Boolean> cached = new ArrayList<>();
        HashSet<Path> filteredPaths = new HashSet<>();
        getAllFilteredPaths(IExpression, filteredPaths);

        for (Path selectedPath : selectedPaths) {
            cached.add(filteredPaths.contains(selectedPath));
        }

        return cached;

    }

    private void getAllFilteredPaths(IExpression IExpression, HashSet<Path> paths) {
        if (IExpression instanceof BinaryExpression) {
            getAllFilteredPaths(((BinaryExpression) IExpression).getLeft(), paths);
            getAllFilteredPaths(((BinaryExpression) IExpression).getRight(), paths);
        } else if (IExpression instanceof SingleSeriesExpression) {
            paths.add(((SingleSeriesExpression) IExpression).getSeriesPath());
        }
    }

}
