package cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.io.Serializable;

/**
 * Define a basic FilterSeries class, which contains deltaObjectUID,
 * measurementUID, seriesDataType and filterType.
 *
 * @author CGF
 */
public abstract class FilterSeries<T extends Comparable<T>> implements Serializable {

    private static final long serialVersionUID = -8834574808000067965L;

    private final String deltaObjectUID;
    private final String measurementUID;
    private final TSDataType seriesDataType;

    // may be TIME_FILTER, FREQUENCY_FILTER, VALUE_FILTER
    private final FilterSeriesType filterType;

    protected FilterSeries(String deltaObjectUID, String measurementUID, TSDataType seriesDataType,
                           FilterSeriesType filterType) {
        this.deltaObjectUID = deltaObjectUID;
        this.measurementUID = measurementUID;
        this.seriesDataType = seriesDataType;
        this.filterType = filterType;
    }

    public String getDeltaObjectUID() {
        return this.deltaObjectUID;
    }

    public String getMeasurementUID() {
        return this.measurementUID;
    }

    public TSDataType getSeriesDataType() {
        return this.seriesDataType;
    }

    public FilterSeriesType getFilterType() {
        return this.filterType;
    }

    @Override
    public String toString() {
        return "FilterSeries (" + deltaObjectUID + "," + measurementUID + "," + seriesDataType + "," + filterType + ")";
    }

    public boolean sameSeries(FilterSeries other) {
        return deltaObjectUID.equals(other.getDeltaObjectUID())
                && measurementUID.equals(other.getMeasurementUID());
    }
}