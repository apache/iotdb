package cn.edu.tsinghua.iotdb.query.dataset;

import cn.edu.tsinghua.iotdb.query.reader.IReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.exception.write.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.common.Field;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.*;

public class EngineDataSetWithoutTimeGenerator extends QueryDataSet {

    private List<IReader> readers;

    private TimeValuePair[] cacheTimeValueList;

    private List<BatchData> batchDataList;

    private List<Boolean> hasDataRemaining;

    private PriorityQueue<Long> timeHeap;

    private Set<Long> timeSet;

    public EngineDataSetWithoutTimeGenerator( List<Path> paths,
                                             List<TSDataType> dataTypes, List<IReader> readers) throws IOException {
        super(paths, dataTypes);
        this.readers = readers;
        initHeap();
    }

    private void initHeap() throws IOException {
        timeSet = new HashSet<>();
        timeHeap = new PriorityQueue<>();
        cacheTimeValueList = new TimeValuePair[readers.size()];

        for (int i = 0; i < readers.size(); i++) {
            IReader reader = readers.get(i);
            if (reader.hasNext()) {
                TimeValuePair timeValuePair = reader.next();
                cacheTimeValueList[i] = timeValuePair;
                timeHeapPut(timeValuePair.getTimestamp());
            }
        }
    }


    @Override
    public boolean hasNext() throws IOException {
        return timeHeap.size() > 0;
    }

    @Override
    public RowRecord next() throws IOException {
        long minTime = timeHeapGet();

        RowRecord record = new RowRecord(minTime);

        for (int i = 0; i < readers.size(); i++) {
            IReader reader = readers.get(i);
            if (cacheTimeValueList[i] == null) {
                record.addField(new Field(null));
            } else {
                if (cacheTimeValueList[i].getTimestamp() == minTime) {
                    record.addField(getField(cacheTimeValueList[i].getValue(), dataTypes.get(i)));
                    if (readers.get(i).hasNext()) {
                        cacheTimeValueList[i] = reader.next();
                        timeHeapPut(cacheTimeValueList[i].getTimestamp());
                    }
                } else {
                    record.addField(new Field(null));
                }
            }
        }

        return record;
    }


    private Field getField(TsPrimitiveType tsPrimitiveType, TSDataType dataType) {
        Field field = new Field(dataType);
        switch (dataType) {
            case INT32:
                field.setIntV(tsPrimitiveType.getInt());
                break;
            case INT64:
                field.setLongV(tsPrimitiveType.getLong());
                break;
            case FLOAT:
                field.setFloatV(tsPrimitiveType.getFloat());
                break;
            case DOUBLE:
                field.setDoubleV(tsPrimitiveType.getDouble());
                break;
            case BOOLEAN:
                field.setBoolV(tsPrimitiveType.getBoolean());
                break;
            case TEXT:
                field.setBinaryV(tsPrimitiveType.getBinary());
                break;
            default:
                throw new UnSupportedDataTypeException("UnSupported: " + dataType);
        }
        return field;
    }


    /**
     * keep heap from storing duplicate time
     */
    private void timeHeapPut(long time) {
        if (!timeSet.contains(time)) {
            timeSet.add(time);
            timeHeap.add(time);
        }
    }

    private Long timeHeapGet() {
        Long t = timeHeap.poll();
        timeSet.remove(t);
        return t;
    }
}
