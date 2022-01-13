package org.apache.iotdb.db.newsync.receiver.load;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageEngineReadonlyException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.SetSystemModePlan;

import java.util.List;


/** This loader is used to load deletion plan. */
public class DeletionPlanLoader implements ILoader{
    @Override
    public boolean load() throws StorageEngineException, LoadFileException, MetadataException {
        return false;
    }

    public void deleteTimeSeries(int startTime, int endTime, List<PartialPath> paths) throws StorageEngineReadonlyException {
        DeletePlan plan = new DeletePlan();
        plan.setDeleteStartTime(startTime);
        plan.setDeleteEndTime(endTime);
        plan.addPaths(paths);
        if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
            throw new StorageEngineReadonlyException();
        }
//        try {
//            StorageEngine.getInstance().delete(path, startTime, endTime, planIndex, timePartitionFilter);
//        } catch (StorageEngineException e) {
//            throw new QueryProcessException(e);
//        }
    }
}
