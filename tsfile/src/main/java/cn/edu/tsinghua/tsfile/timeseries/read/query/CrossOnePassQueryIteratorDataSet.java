package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.OldRowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * This class is the subclass of {@code OnePassQueryDataSet}. It is used to store
 * and fetch more records for batch query in TsFile's SingleFileQuery.
 *
 * @author Jinrui Zhang
 */
public abstract class CrossOnePassQueryIteratorDataSet extends OnePassQueryDataSet {
    private static final Logger LOG = LoggerFactory.getLogger(CrossOnePassQueryIteratorDataSet.class);
    //special for save time values when processing cross getIndex
    private boolean hasReadAll;

    public CrossOnePassQueryIteratorDataSet(CrossQueryTimeGenerator timeGenerator) throws IOException {
        this.crossQueryTimeGenerator = timeGenerator;
        mapRet = new LinkedHashMap<>();
        hasReadAll = getMoreRecords();
        size = mapRet.size();
    }

    /**
     * @return True represents that there is no more data to be read.
     * @throws IOException exception in IO
     */
    public abstract boolean getMoreRecords() throws IOException;

    public boolean hasNextRecord() {
        if (!ifInit) {
            // hasReadAll is true represents that there is no records in this OnePassQueryDataSet
            if (hasReadAll) {
                return false;
            }
            initForRecord();
            ifInit = true;
        }
        if (heap.peek() != null) {
            return true;
        }
        if (!hasReadAll) {
            try {
                hasReadAll = getMoreRecords();
                if (hasReadAll) {
                    return false;
                }
                initForRecord();
                if (heap.peek() != null) {
                    return true;
                }
            } catch (IOException e) {
                LOG.error("Error in get Next Record:", e);
            }
        }
        return false;
    }

    public OldRowRecord getNextRecord() {
        if (!hasNextRecord()) {
            return null;
        }

        Long minTime = heapGet();
        OldRowRecord r = new OldRowRecord(minTime, null, null);
        for (int i = 0; i < size; i++) {
            if (i == 0) {
                r.setDeltaObjectId(deltaObjectIds[i]);
            }
            Field f;

            //get more fields in columns i
            if (timeIdxs[i] < cols[i].valueLength) {
                //Get more fields from file...
            }

            if (timeIdxs[i] < cols[i].valueLength && minTime == cols[i].getTime(timeIdxs[i])) {
                f = new Field(cols[i].dataType, deltaObjectIds[i], measurementIds[i]);
                f.setNull(false);
                putValueToField(cols[i], timeIdxs[i], f);
                timeIdxs[i]++;
                if (timeIdxs[i] < cols[i].valueLength) {
                    heapPut(cols[i].getTime(timeIdxs[i]));
                }
            } else {
                f = new Field(cols[i].dataType, measurementIds[i]);
                f.setNull(true);
            }
            r.addField(f);
        }
        return r;
    }
}