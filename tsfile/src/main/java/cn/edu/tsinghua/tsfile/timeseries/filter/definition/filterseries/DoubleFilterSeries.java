package cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * The value type of DoubleFilterSeries is Double.
 *
 * @author CGF
 */
public class DoubleFilterSeries extends FilterSeries<Double> {

    private static final long serialVersionUID = -5847065869887482598L;

    public DoubleFilterSeries(String deltaObjectUID, String measurementUID, TSDataType seriesDataType,
                              FilterSeriesType filterType) {
        super(deltaObjectUID, measurementUID, TSDataType.DOUBLE, filterType);
    }
}
