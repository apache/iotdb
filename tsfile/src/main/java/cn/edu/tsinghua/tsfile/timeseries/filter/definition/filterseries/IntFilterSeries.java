package cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * The value type of IntFilterSeries is Integer.
 *
 * @author CGF
 */
public class IntFilterSeries extends FilterSeries<Integer> {

    private static final long serialVersionUID = -7268852368134017134L;

    public IntFilterSeries(String deltaObjectUID, String measurementUID, TSDataType seriesDataType,
                           FilterSeriesType filterType) {
        super(deltaObjectUID, measurementUID, TSDataType.INT32, filterType);
    }
}