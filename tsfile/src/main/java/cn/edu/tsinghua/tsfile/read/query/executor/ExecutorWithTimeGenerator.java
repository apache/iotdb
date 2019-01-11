package cn.edu.tsinghua.tsfile.read.query.executor;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.BinaryExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.DataSetWithTimeGenerator;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.TimeGenerator;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.TimeGeneratorImpl;
import cn.edu.tsinghua.tsfile.read.reader.series.SeriesReaderByTimestamp;

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
     * All leaf nodes of queryFilter in queryExpression are SeriesFilters,
     * We use a TimeGenerator to control query processing.
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

        for(int i = 0; i < cached.size(); i++) {

            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(selectedPathList.get(i));
            dataTypes.add(chunkMetaDataList.get(0).getTsDataType());

            if(cached.get(i)) {
                readersOfSelectedSeries.add(null);
                continue;
            }

            SeriesReaderByTimestamp seriesReader = new SeriesReaderByTimestamp(chunkLoader, chunkMetaDataList);
            readersOfSelectedSeries.add(seriesReader);
        }

        return new DataSetWithTimeGenerator(selectedPathList, cached, dataTypes, timeGenerator, readersOfSelectedSeries);
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
