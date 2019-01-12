package org.apache.iotdb.db.writelog.replay;

import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;

import java.util.List;

public class ConcreteLogReplayer implements LogReplayer {

    public void replay(PhysicalPlan plan) throws ProcessorException {
        try {
            if (plan instanceof InsertPlan) {
                InsertPlan insertPlan = (InsertPlan) plan;
                multiInsert(insertPlan);
            } else if (plan instanceof UpdatePlan) {
                UpdatePlan updatePlan = (UpdatePlan) plan;
                update(updatePlan);
            } else if (plan instanceof DeletePlan) {
                DeletePlan deletePlan = (DeletePlan) plan;
                delete(deletePlan);
            }
        } catch (Exception e) {
            throw new ProcessorException(String.format("Cannot replay log %s, because %s", plan.toString(), e.getMessage()));
        }
    }

    private void multiInsert(InsertPlan insertPlan)
            throws PathErrorException, FileNodeManagerException {
        String deviceId = insertPlan.getDeviceId();
        long insertTime = insertPlan.getTime();
        List<String> measurementList = insertPlan.getMeasurements();
        List<String> insertValues = insertPlan.getValues();

        TSRecord tsRecord = new TSRecord(insertTime, deviceId);
        for (int i = 0; i < measurementList.size(); i++) {
            String pathKey = deviceId + "." + measurementList.get(i);
            TSDataType dataType = MManager.getInstance().getSeriesType(pathKey);
            String value = insertValues.get(i);
            DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementList.get(i), value);
            tsRecord.addTuple(dataPoint);
        }
        FileNodeManager.getInstance().insert(tsRecord,true);
    }

    private void update(UpdatePlan updatePlan) throws FileNodeManagerException, PathErrorException {
        TSDataType dataType = MManager.getInstance().getSeriesType(updatePlan.getPath().getFullPath());
        for (Pair<Long, Long> timePair : updatePlan.getIntervals()) {
            FileNodeManager.getInstance().update(updatePlan.getPath().getDevice(), updatePlan.getPath().getMeasurement(),
                    timePair.left, timePair.right, dataType, updatePlan.getValue());
        }
    }

    private void delete(DeletePlan deletePlan) throws FileNodeManagerException, PathErrorException {
        MManager mManager = MManager.getInstance();
        for (Path path : deletePlan.getPaths()) {
            FileNodeManager.getInstance().delete(path.getDevice(), path.getMeasurement(),
                    deletePlan.getDeleteTime(), mManager.getSeriesType(path.getFullPath()));
        }
    }
}
