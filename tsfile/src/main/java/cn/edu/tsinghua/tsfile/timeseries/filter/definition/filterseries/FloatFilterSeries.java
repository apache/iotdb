package cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * The value type of FloatFilterSeries is Float.
 *
 * @author CGF
 */
public class FloatFilterSeries extends FilterSeries<Float> {

    private static final long serialVersionUID = -2745416005497409478L;

    public FloatFilterSeries(String deltaObjectUID, String measurementUID, TSDataType seriesDataType,
                             FilterSeriesType filterType) {
        super(deltaObjectUID, measurementUID, TSDataType.FLOAT, filterType);
    }
}
