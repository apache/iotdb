package cn.edu.tsinghua.tsfile.read.query.executor;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.exception.filter.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.GlobalTimeExpression;
import cn.edu.tsinghua.tsfile.read.expression.util.ExpressionOptimizer;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.DataSetWithoutTimeGenerator;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReader;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class TsFileExecutor implements QueryExecutor {

    private MetadataQuerier metadataQuerier;
    private ChunkLoader chunkLoader;

    public TsFileExecutor(MetadataQuerier metadataQuerier, ChunkLoader chunkLoader) {
        this.metadataQuerier = metadataQuerier;
        this.chunkLoader = chunkLoader;
    }

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {

        metadataQuerier.loadChunkMetaDatas(queryExpression.getSelectedSeries());

        if (queryExpression.hasQueryFilter()) {
            try {
                IExpression expression = queryExpression.getExpression();
                IExpression regularIExpression = ExpressionOptimizer.getInstance().optimize(expression, queryExpression.getSelectedSeries());
                queryExpression.setExpression(regularIExpression);

                if (regularIExpression instanceof GlobalTimeExpression) {
                    return execute(queryExpression.getSelectedSeries(), (GlobalTimeExpression) regularIExpression);
                } else {
                    return new ExecutorWithTimeGenerator(metadataQuerier, chunkLoader).execute(queryExpression);
                }
            } catch (QueryFilterOptimizationException e) {
                throw new IOException(e);
            }
        } else {
            return execute(queryExpression.getSelectedSeries());
        }
    }


    /**
     * no filter, can use multi-way merge
     *
     * @param selectedPathList all selected paths
     * @return DataSet without TimeGenerator
     */
    private QueryDataSet execute(List<Path> selectedPathList) throws IOException {
        List<FileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();

        for (Path path : selectedPathList) {
            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
            FileSeriesReader seriesReader = new FileSeriesReaderWithoutFilter(chunkLoader, chunkMetaDataList);
            readersOfSelectedSeries.add(seriesReader);
            dataTypes.add(chunkMetaDataList.get(0).getTsDataType());
        }
        return new DataSetWithoutTimeGenerator(selectedPathList, dataTypes, readersOfSelectedSeries);
    }


    /**
     * has a GlobalTimeExpression, can use multi-way merge
     *
     * @param selectedPathList all selected paths
     * @param timeFilter       GlobalTimeExpression that takes effect to all selected paths
     * @return DataSet without TimeGenerator
     */
    private QueryDataSet execute(List<Path> selectedPathList, GlobalTimeExpression timeFilter) throws IOException {
        List<FileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();

        for (Path path : selectedPathList) {
            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
            FileSeriesReader seriesReader = new FileSeriesReaderWithFilter(chunkLoader, chunkMetaDataList, timeFilter.getFilter());
            readersOfSelectedSeries.add(seriesReader);
            dataTypes.add(chunkMetaDataList.get(0).getTsDataType());
        }

        return new DataSetWithoutTimeGenerator(selectedPathList, dataTypes, readersOfSelectedSeries);
    }


}
