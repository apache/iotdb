package cn.edu.tsinghua.iotdb.index.kvmatch;

import cn.edu.fudan.dsm.kvmatch.iotdb.common.QueryConfig;
import cn.edu.fudan.dsm.kvmatch.iotdb.utils.SeriesUtils;
import cn.edu.tsinghua.iotdb.query.management.ReadCachePrefix;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import cn.edu.tsinghua.iotdb.index.common.QueryDataSetIterator;
import cn.edu.tsinghua.iotdb.query.engine.OverflowQueryEngine;
import cn.edu.tsinghua.iotdb.query.reader.RecordReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * The class validates the candidates to find out actual matching results satisfying the query request.
 *
 * @author Jiaye Wu
 */
public class KvMatchCandidateValidator implements Callable<List<Pair<Pair<Long, Long>, Double>>> {

    private static final Logger logger = LoggerFactory.getLogger(KvMatchCandidateValidator.class);

    private Path columnPath;

    private List<Pair<Long, Long>> scanIntervals;

    private QueryConfig queryConfig;

    private int token;

    public KvMatchCandidateValidator(Path columnPath, List<Pair<Long, Long>> scanIntervals, QueryConfig queryConfig, int token) {
        this.columnPath = new Path(columnPath.getFullPath());
        this.scanIntervals = scanIntervals;
        this.queryConfig = queryConfig;
        this.token = token;
    }

    @Override
    public List<Pair<Pair<Long, Long>, Double>> call() throws Exception {
        logger.info("Validating candidate intervals: {}", scanIntervals);

        QueryDataSetIterator queryDataSetIterator = new QueryDataSetIterator(new OverflowQueryEngine(), columnPath, scanIntervals, token);

        List<Pair<Pair<Long, Long>, Double>> result = new ArrayList<>();
        Pair<Long, Double> lastKeyPoint = null;
        for (Pair<Long, Long> scanInterval : scanIntervals) {
            List<Pair<Long, Double>> keyPoints = new ArrayList<>();
            while (queryDataSetIterator.hasNext()) {
                RowRecord row = queryDataSetIterator.getRowRecord();
                double value = SeriesUtils.getValue(row.getFields().get(0));  // one column only
                if (keyPoints.isEmpty() && row.getTime() > scanInterval.left) {
                    if (lastKeyPoint == null) {
                        keyPoints.add(new Pair<>(scanInterval.left, value));
                    } else {
                        keyPoints.add(lastKeyPoint);
                    }
                }
                keyPoints.add(new Pair<>(row.getTime(), value));
                if (row.getTime() >= scanInterval.right) break;
            }
            if (keyPoints.isEmpty()) break;
            lastKeyPoint = keyPoints.get(keyPoints.size() - 1);
            List<Double> series = SeriesUtils.amend(keyPoints, scanInterval);

            double ex = 0, ex2 = 0;
            int lenQ = queryConfig.getQuerySeries().size(), idx = 0;
            double[] circularArray = new double[2 * lenQ];
            for (int i = 0; i < series.size(); i++) {
                double value = series.get(i);
                ex += value;
                ex2 += value * value;
                circularArray[i % lenQ] = value;
                circularArray[(i % lenQ) + lenQ] = value;

                if (i >= lenQ - 1) {
                    int j = (i + 1) % lenQ;  // the current starting location of T

                    long left = scanInterval.left + i - lenQ + 1;
                    if (left == keyPoints.get(idx).left) {  // remove non-exist timestamp
                        idx++;

                        if (queryConfig.isNormalization()) {
                            double mean = ex / lenQ;  // z
                            double std = Math.sqrt(ex2 / lenQ - mean * mean);

                            if (Math.abs(mean - queryConfig.getMeanQ()) <= queryConfig.getBeta() && std / queryConfig.getStdQ() <= queryConfig.getBeta() && std / queryConfig.getStdQ() >= 1.0 / queryConfig.getAlpha()) {
                                double dist = 0;
                                for (int k = 0; k < lenQ && dist <= queryConfig.getEpsilon() * queryConfig.getEpsilon(); k++) {
                                    double x = (circularArray[(queryConfig.getOrder().get(k) + j)] - mean) / std;
                                    dist += (x - queryConfig.getNormalizedQuerySeries().get(k)) * (x - queryConfig.getNormalizedQuerySeries().get(k));
                                }
                                if (dist <= queryConfig.getEpsilon() * queryConfig.getEpsilon()) {
                                    result.add(new Pair<>(new Pair<>(left, scanInterval.left + i), Math.sqrt(dist)));
                                }
                            }
                        } else {
                            double dist = 0;
                            for (int k = 0; k < lenQ && dist <= queryConfig.getEpsilon() * queryConfig.getEpsilon(); k++) {
                                double x = circularArray[k + j];
                                dist += (x - queryConfig.getQuerySeries().get(k)) * (x - queryConfig.getQuerySeries().get(k));
                            }
                            if (dist <= queryConfig.getEpsilon() * queryConfig.getEpsilon()) {
                                result.add(new Pair<>(new Pair<>(left, scanInterval.left + i), Math.sqrt(dist)));
                            }
                        }
                    }

                    ex -= circularArray[j];
                    ex2 -= circularArray[j] * circularArray[j];
                }
            }
        }
        String prefix = ReadCachePrefix.addQueryPrefix(0);
        RecordReaderFactory.getInstance().removeRecordReader(prefix + columnPath.getDeltaObjectToString(), columnPath.getMeasurementToString());

        logger.info("Finished validating candidate intervals: {}", scanIntervals);
        return result;
    }
}
