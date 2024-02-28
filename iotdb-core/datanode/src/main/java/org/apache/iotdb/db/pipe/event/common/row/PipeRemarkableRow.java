package org.apache.iotdb.db.pipe.event.common.row;

public class PipeRemarkableRow extends PipeRow {
  public PipeRemarkableRow(PipeRow pipeRow) {
    super(
        pipeRow.getRowIndex(),
        pipeRow.getDeviceId(),
        pipeRow.isAligned(),
        pipeRow.getMeasurementSchemaList(),
        pipeRow.getTimestampColumn(),
        pipeRow.getValueColumnTypes(),
        pipeRow.getValueColumns(),
        pipeRow.getBitMaps(),
        pipeRow.getColumnNameStringList());
  }

  public void remark(int index) {
    bitMaps[index].mark(getRowIndex());
  }
}
