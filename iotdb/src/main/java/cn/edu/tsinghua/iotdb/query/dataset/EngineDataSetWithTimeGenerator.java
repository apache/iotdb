package cn.edu.tsinghua.iotdb.query.dataset;

import cn.edu.tsinghua.iotdb.query.reader.merge.EngineReaderByTimeStamp;
import cn.edu.tsinghua.iotdb.query.timegenerator.EngineTimeGenerator;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.exception.write.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.Field;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.utils.Binary;

import java.io.IOException;
import java.util.List;


public class EngineDataSetWithTimeGenerator extends QueryDataSet {

    private EngineTimeGenerator timeGenerator;
    private List<EngineReaderByTimeStamp> readers;

    public EngineDataSetWithTimeGenerator(List<Path> paths, List<TSDataType> dataTypes,
                                          EngineTimeGenerator timeGenerator, List<EngineReaderByTimeStamp> readers) {
        super(paths, dataTypes);
        this.timeGenerator = timeGenerator;
        this.readers = readers;
    }


    @Override
    public boolean hasNext() throws IOException {
        return timeGenerator.hasNext();
    }


    @Override
    public RowRecord next() throws IOException {
        long timestamp = timeGenerator.next();
        RowRecord rowRecord = new RowRecord(timestamp);
        for (int i = 0; i < readers.size(); i++) {
            EngineReaderByTimeStamp reader = readers.get(i);
            TsPrimitiveType tsPrimitiveType = reader.getValueInTimestamp(timestamp);
            if (tsPrimitiveType == null) {
                rowRecord.addField(new Field(null));
            } else {
                rowRecord.addField(getField(tsPrimitiveType.getValue(), dataTypes.get(i)));
            }
        }

        return rowRecord;
    }

    private Field getField(Object value, TSDataType dataType) {
        Field field = new Field(dataType);

        if (value == null) {
            field.setNull();
            return field;
        }

        switch (dataType) {
            case DOUBLE:
                field.setDoubleV((double) value);
                break;
            case FLOAT:
                field.setFloatV((float) value);
                break;
            case INT64:
                field.setLongV((long) value);
                break;
            case INT32:
                field.setIntV((int) value);
                break;
            case BOOLEAN:
                field.setBoolV((boolean) value);
                break;
            case TEXT:
                field.setBinaryV((Binary) value);
                break;
            default:
                throw new UnSupportedDataTypeException("UnSupported: " + dataType);
        }
        return field;
    }
}
