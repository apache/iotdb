package cn.edu.thu.tsfiledb.query.aggregation.impl;

import java.io.IOException;

import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.Digest;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

public class MaxValueAggrFunc extends AggregateFunction {

    private boolean hasSetValue = false;

    public MaxValueAggrFunc(TSDataType dataType) {
        super("MAX_VALUE", dataType);
        result.data.putTime(0);
    }

    @Override
    public boolean calculateValueFromPageHeader(PageHeader pageHeader) {
        Digest pageDigest = pageHeader.data_page_header.getDigest();
        DigestForFilter digest = new DigestForFilter(pageDigest.min, pageDigest.max, dataType);
        Comparable<?> maxv = digest.getMaxValue();
        if (!hasSetValue) {
            result.data.putAnObject(maxv);
            hasSetValue = true;
        } else {
            Comparable<?> v = result.data.getAnObject(0);
            if (compare(v, maxv) < 0) {
                result.data.setAnObject(0, maxv);
            }
        }
        return true;
    }

    @Override
    public void calculateValueFromDataInThisPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        if (dataInThisPage instanceof InsertDynamicData) {
            Object max_value = ((InsertDynamicData) dataInThisPage).calcAggregation("MAX_VALUE");
            if (max_value != null) {
                if (!hasSetValue) {
                    result.data.putAnObject(max_value);
                    hasSetValue = true;
                } else {
                    Comparable<?> v = result.data.getAnObject(0);
                    if (compare(v, (Comparable<?>)max_value) < 0) {
                        result.data.setAnObject(0, (Comparable<?>)max_value);
                    }
                }
            }
        } else {
            if (dataInThisPage.valueLength == 0) {
                return;
            }
            Comparable<?> maxv = getMaxValue(dataInThisPage);
            if (!hasSetValue) {
                result.data.putAnObject(maxv);
                hasSetValue = true;
            } else {
                Comparable<?> v = result.data.getAnObject(0);
                if (compare(v, maxv) < 0) {
                    result.data.setAnObject(0, maxv);
                }
            }
        }
    }

    private Comparable<?> getMaxValue(DynamicOneColumnData dataInThisPage) {
        Comparable<?> v = dataInThisPage.getAnObject(0);
        for (int i = 1; i < dataInThisPage.valueLength; i++) {
            Comparable<?> nextV = dataInThisPage.getAnObject(i);
            if (compare(v, nextV) < 0) {
                v = nextV;
            }
        }
        return v;
    }

    private int compare(Comparable<?> o1, Comparable<?> o2) {
        switch (dataType) {
            case BOOLEAN:
                return ((Boolean) o1).compareTo((Boolean) o2);
            case INT32:
                return ((Integer) o1).compareTo((Integer) o2);
            case INT64:
                return ((Long) o1).compareTo((Long) o2);
            case FLOAT:
                return ((Float) o1).compareTo((Float) o2);
            case DOUBLE:
                return ((Double) o1).compareTo((Double) o2);
            case TEXT:
                return ((Binary) o1).compareTo((Binary) o2);
            default:
                throw new UnSupportedDataTypeException("Aggregation UnSupportDataType: " + dataType);
        }
    }

}
