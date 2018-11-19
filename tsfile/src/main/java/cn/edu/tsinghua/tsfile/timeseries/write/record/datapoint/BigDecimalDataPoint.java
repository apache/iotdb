package cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.series.ISeriesWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * a subclass for BigDecimal data type extends DataPoint
 *
 * @author kangrong
 * @see DataPoint DataPoint
 */
public class BigDecimalDataPoint extends DataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(BigDecimalDataPoint.class);
    private BigDecimal value;

    public BigDecimalDataPoint(String measurementId, BigDecimal v) {
        super(TSDataType.BIGDECIMAL, measurementId);
        this.value = v;
    }

    @Override
    public void write(long time, ISeriesWriter writer) throws IOException {
        if (writer == null) {
            LOG.warn("given ISeriesWriter is null, do nothing and return");
            return;
        }
        writer.write(time, value);
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public void setBigDecimal(BigDecimal value) {
        this.value = value;
    }
}
