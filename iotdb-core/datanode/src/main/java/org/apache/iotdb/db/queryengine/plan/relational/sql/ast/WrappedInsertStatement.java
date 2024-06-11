package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import java.util.List;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.execution.schedule.queue.ID;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaComputationWithAutoCreation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;

public abstract class WrappedInsertStatement extends WrappedStatement {

  protected InsertBaseStatement insertStatement;

  public WrappedInsertStatement(
      InsertBaseStatement innerTreeStatement,
      MPPQueryContext context) {
    super(innerTreeStatement, context);
    this.insertStatement = innerTreeStatement;
  }

  @Override
  public InsertBaseStatement getInnerTreeStatement() {
    return insertStatement;
  }

  public abstract List<ISchemaComputationWithAutoCreation> getSchemaValidationList();

  public abstract void updateAfterSchemaValidation(MPPQueryContext context) throws QueryProcessException;

  public abstract ISchemaComputationWithAutoCreation getSchemaComputation(IDeviceID deviceID);

  public abstract class BasicSchemaExecutions implements ISchemaComputationWithAutoCreation {
    protected IDeviceID deviceID;

    public BasicSchemaExecutions(IDeviceID deviceID) {
      this.deviceID = deviceID;
    }

    @Override
    public boolean isAligned() {
      return true;
    }

    @Override
    public TSDataType getDataType(int index) {
      return insertStatement.getDataTypes()[index];
    }

    @Override
    public TSEncoding getEncoding(int index) {
      return null;
    }

    @Override
    public CompressionType getCompressionType(int index) {
      return null;
    }

    @Override
    public PartialPath getDevicePath() {
      //TODO-TableInsertion: use deviceId
      try {
        return new PartialPath(deviceID);
      } catch (IllegalPathException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String[] getMeasurements() {
      return insertStatement.getMeasurements();
    }

    @Override
    public void computeDevice(boolean isAligned) {
      // ignored, table device must be aligned
    }

  }
}
