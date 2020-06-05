package org.apache.iotdb.db.nvm.memtable;

import java.util.List;
import org.apache.iotdb.db.engine.memtable.AbstractWritableMemChunk;
import org.apache.iotdb.db.nvm.space.NVMDataSpace;
import org.apache.iotdb.db.utils.datastructure.NVMTVList;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class NVMWritableMemChunk extends AbstractWritableMemChunk {

  public NVMWritableMemChunk(MeasurementSchema schema, NVMTVList list) {
    this.schema = schema;
    this.list = list;
  }

  public void loadData(List<NVMDataSpace> timeSpaceList, List<NVMDataSpace> valueSpaceList) {
    ((NVMTVList) list).loadData(timeSpaceList, valueSpaceList);
  }
}
