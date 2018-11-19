package cn.edu.tsinghua.tsfile.timeseries.write.series;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.NoMeasurementException;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.IPageWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.PageWriterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;

/**
 * a implementation of IRowGroupWriter
 *
 * @author kangrong
 * @see IRowGroupWriter IRowGroupWriter
 */
public class RowGroupWriterImpl implements IRowGroupWriter {
    private static Logger LOG = LoggerFactory.getLogger(RowGroupWriterImpl.class);
    private final String deltaObjectId;
    private Map<String, ISeriesWriter> dataSeriesWriters = new HashMap<String, ISeriesWriter>();

    public RowGroupWriterImpl(String deltaObjectId) {
        this.deltaObjectId = deltaObjectId;
    }

    @Override
    public void addSeriesWriter(MeasurementDescriptor desc, int pageSizeThreshold) {
        if(!dataSeriesWriters.containsKey(desc.getMeasurementId())) {
            IPageWriter pageWriter = new PageWriterImpl(desc);
            ISeriesWriter seriesWriter = new SeriesWriterImpl(deltaObjectId, desc, pageWriter, pageSizeThreshold);
            this.dataSeriesWriters.put(desc.getMeasurementId(), seriesWriter);
        }
    }

    @Override
    public void write(long time, List<DataPoint> data) throws WriteProcessException, IOException {
        for (DataPoint point : data) {
            String measurementId = point.getMeasurementId();
            if (!dataSeriesWriters.containsKey(measurementId))
                throw new NoMeasurementException("time " + time + ", measurement id " + measurementId + " not found!");
            point.write(time, dataSeriesWriters.get(measurementId));

        }
    }

    @Override
    public void flushToFileWriter(TsFileIOWriter deltaFileWriter) throws IOException {
        LOG.debug("start flush delta object id:{}", deltaObjectId);
        for (ISeriesWriter seriesWriter : dataSeriesWriters.values()) {
            seriesWriter.writeToFileWriter(deltaFileWriter);
        }
    }

    @Override
    public long updateMaxGroupMemSize() {
        long bufferSize = 0;
        for (ISeriesWriter seriesWriter : dataSeriesWriters.values())
            bufferSize += seriesWriter.estimateMaxSeriesMemSize();
        return bufferSize;
    }
}
