package cn.edu.tsinghua.iotdb.writelog.replay;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.InsertPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

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
        FileNodeManager.getInstance().insert(tsRecord,true);
    }

    private void update(UpdatePlan updatePlan) throws FileNodeManagerException, PathErrorException {
        TSDataType dataType = MManager.getInstance().getSeriesType(updatePlan.getPath().getFullPath());
        for (Pair<Long, Long> timePair : updatePlan.getIntervals()) {
            FileNodeManager.getInstance().update(updatePlan.getPath().getDeltaObjectToString(), updatePlan.getPath().getMeasurementToString(),
                    timePair.left, timePair.right, dataType, updatePlan.getValue());
        }
    }

    private void delete(DeletePlan deletePlan) throws FileNodeManagerException, PathErrorException {
        MManager mManager = MManager.getInstance();
        for (Path path : deletePlan.getPaths()) {
            FileNodeManager.getInstance().delete(path.getDeltaObjectToString(), path.getMeasurementToString(),
                    deletePlan.getDeleteTime(), mManager.getSeriesType(path.getFullPath()));
        }
    }
}
