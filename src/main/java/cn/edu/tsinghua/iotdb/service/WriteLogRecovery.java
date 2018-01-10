package cn.edu.tsinghua.iotdb.service;

import java.util.List;
import java.util.logging.Logger;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.monitor.MonitorConstants;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.InsertPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

/**
 * This class is used for system recovery.
 * Invoking method of <code>FileNodeManager</code> directly, not to use <code>OverflowQpExecutor</code>,
 * to avoid the examine of <code>OverflowQpExecutor</code>.
 *
 * @author CGF.
 */
class WriteLogRecovery {

    static void multiInsert(InsertPlan insertPlan)
            throws ProcessorException, PathErrorException, FileNodeManagerException {
        String deltaObject = insertPlan.getDeltaObject();
        long insertTime = insertPlan.getTime();
        List<String> measurementList = insertPlan.getMeasurements();
        List<String> insertValues = insertPlan.getValues();

        TSRecord tsRecord = new TSRecord(insertTime, deltaObject);
        for (int i = 0; i < measurementList.size(); i++) {
            String pathKey = deltaObject + "." + measurementList.get(i);
            TSDataType dataType = MManager.getInstance().getSeriesType(pathKey);
            String value = insertValues.get(i);
            DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementList.get(i), value);
            tsRecord.addTuple(dataPoint);
        }
        String fileName = MManager.getInstance().getFileNameByPath(deltaObject);
        // When Wal restores the statistics info, need to set isMonitor true for the insert method to stop
        // collecting statistics data of storage group whose name is  "root.stats". Because system will not
        // Record statistics data for "root.stats" storage group
        if (MonitorConstants.statStorageGroupPrefix.equals(fileName)) {
            FileNodeManager.getInstance().insert(tsRecord, true);
        } else {
            FileNodeManager.getInstance().insert(tsRecord, false);
        }
    }

    static void update(UpdatePlan updatePlan) throws FileNodeManagerException, PathErrorException {
        TSDataType dataType = MManager.getInstance().getSeriesType(updatePlan.getPath().getFullPath());
        for (Pair<Long, Long> timePair : updatePlan.getIntervals()) {
            FileNodeManager.getInstance().update(updatePlan.getPath().getDeltaObjectToString(), updatePlan.getPath().getMeasurementToString(),
                    timePair.left, timePair.right, dataType, updatePlan.getValue());
        }
    }

    static void delete(DeletePlan deletePlan) throws FileNodeManagerException, PathErrorException, ProcessorException {
        MManager mManager = MManager.getInstance();
        for (Path path : deletePlan.getPaths()) {
            FileNodeManager.getInstance().delete(path.getDeltaObjectToString(), path.getMeasurementToString(),
                    deletePlan.getDeleteTime(), mManager.getSeriesType(path.getFullPath()));
        }
    }
}
