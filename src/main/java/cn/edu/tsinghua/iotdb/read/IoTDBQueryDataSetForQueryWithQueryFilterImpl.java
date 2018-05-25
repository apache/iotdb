package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;

import java.io.IOException;
import java.util.LinkedHashMap;

public class IoTDBQueryDataSetForQueryWithQueryFilterImpl implements QueryDataSet {

    private TimestampGenerator timestampGenerator;
    private LinkedHashMap<Path, SeriesReaderByTimeStamp> readersOfSelectedSeries;

    public IoTDBQueryDataSetForQueryWithQueryFilterImpl(TimestampGenerator timestampGenerator, LinkedHashMap<Path, SeriesReaderByTimeStamp> readersOfSelectedSeries){
        this.timestampGenerator = timestampGenerator;
        this.readersOfSelectedSeries = readersOfSelectedSeries;
    }

    @Override
    public boolean hasNext() throws IOException {
        return timestampGenerator.hasNext();
    }

    @Override
    public RowRecord next() throws IOException {
        long timestamp = timestampGenerator.next();
        RowRecord rowRecord = new RowRecord(timestamp);
        for (Path path : readersOfSelectedSeries.keySet()) {
            SeriesReaderByTimeStamp seriesReaderByTimestamp = readersOfSelectedSeries.get(path);
            seriesReaderByTimestamp.setCurrentTimestamp(timestamp);
            if(seriesReaderByTimestamp.hasNext()){
                rowRecord.putField(path, seriesReaderByTimestamp.next().getValue());
            }
            else {
                rowRecord.putField(path, null);
            }
        }
        return rowRecord;
    }
}
