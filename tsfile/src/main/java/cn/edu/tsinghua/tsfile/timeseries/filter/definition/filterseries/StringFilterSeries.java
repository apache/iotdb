package cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * The value type of BooleanFilterSeries is Boolean.
 *
 * @author CGF
 */
public class StringFilterSeries extends FilterSeries<Binary> {

    private static final long serialVersionUID = 454794989741185890L;

    public StringFilterSeries(String deltaObjectUID, String measurementUID, TSDataType seriesDataType,
                              FilterSeriesType filterType) {
        super(deltaObjectUID, measurementUID, TSDataType.TEXT, filterType);
    }
}
