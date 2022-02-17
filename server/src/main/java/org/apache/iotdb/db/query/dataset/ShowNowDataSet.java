package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.ShowNowPlan;
import org.apache.iotdb.db.qp.utils.ShowNowUtils;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.*;

public class ShowNowDataSet extends ShowDataSet{
    private final QueryContext context;
    private static final Path[] resourcePaths = {
            new PartialPath(IP_ADDRESS, false),
            new PartialPath(SYSTEM_TIME, false),
            new PartialPath(CPU_LOAD, false),
            new PartialPath(TOTAL_MEMORYSIZE, false),
            new PartialPath(FREE_MEMORYSIZE, false),
    };

    private static final TSDataType[] resourceTypes = {
            TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT
    };

    protected ShowNowDataSet(List<Path> paths, List<TSDataType> dataTypes, QueryContext context) {
        super(paths, dataTypes);
        this.context = context;
    }

    public ShowNowDataSet(ShowNowPlan plan, QueryContext context) throws MetadataException {
        super(Arrays.asList(resourcePaths), Arrays.asList(resourceTypes));
        this.plan =  plan;
        this.context = context;
        getQueryDataSet();
    }

    public ShowNowDataSet(
            List<ShowNowResult> showNowResults, QueryContext context, ShowNowPlan showNowPlan)
            throws MetadataException {
        super(Arrays.asList(resourcePaths), Arrays.asList(resourceTypes));
        this.context = context;
        this.plan = showNowPlan;
        getQueryDataSet(showNowResults);
    }

    @Override
    public List<RowRecord> getQueryDataSet() throws MetadataException {

        List<ShowNowResult> showNowResults = new ShowNowUtils().getShowNowResults();

        List<RowRecord> records = new ArrayList<>();
        for (ShowNowResult result : showNowResults) {
            RowRecord record = new RowRecord(0);
            updateRecord(record, result.getIp());
            updateRecord(record, result.getSystemTime());
            updateRecord(record, result.getCpuLoad());
            updateRecord(record, result.getTotalMemroySize());
            updateRecord(record, result.getFreeMemroySize());
            records.add(record);
            putRecord(record);
        }
        return records;
    }

    public List<RowRecord> getQueryDataSet(List<ShowNowResult> showNowResults) throws MetadataException {

        List<RowRecord> records = new ArrayList<>();

        for (ShowNowResult result : showNowResults) {
            RowRecord record = new RowRecord(0);
            updateRecord(record, result.getIp());
            updateRecord(record, result.getSystemTime());
            updateRecord(record, result.getCpuLoad());
            updateRecord(record, result.getTotalMemroySize());
            updateRecord(record, result.getFreeMemroySize());
            records.add(record);
            putRecord(record);
        }
        return records;
    }
}
