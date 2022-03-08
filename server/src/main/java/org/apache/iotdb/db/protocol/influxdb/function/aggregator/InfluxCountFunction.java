package org.apache.iotdb.db.protocol.influxdb.function.aggregator;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.utils.InfluxDBUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class InfluxCountFunction extends InfluxAggregator {
  private int countNum = 0;

  public InfluxCountFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  public InfluxCountFunction(
      List<Expression> expressionList, String path, ServiceProvider serviceProvider) {
    super(expressionList, path, serviceProvider);
  }

  @Override
  public InfluxFunctionValue calculate() {
    return new InfluxFunctionValue(this.countNum, 0L);
  }

  @Override
  public InfluxFunctionValue calculateByIoTDBFunc() {
    int count = 0;
    long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
    try {
      String functionSql = InfluxDBUtils.generateFunctionSql("count", getParmaName(), path);
      QueryPlan queryPlan =
          (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(functionSql);
      QueryContext queryContext =
          serviceProvider.genQueryContext(
              queryId,
              true,
              System.currentTimeMillis(),
              functionSql,
              IoTDBConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
      QueryDataSet queryDataSet =
          serviceProvider.createQueryDataSet(
              queryContext, queryPlan, IoTDBConstant.DEFAULT_FETCH_SIZE);
      while (queryDataSet.hasNext()) {
        List<Field> fields = queryDataSet.next().getFields();
        if (fields.get(1).getDataType() != null) {
          count += fields.get(1).getLongV();
        }
      }
    } catch (QueryProcessException
        | TException
        | StorageEngineException
        | SQLException
        | IOException
        | InterruptedException
        | QueryFilterOptimizationException
        | MetadataException e) {
      throw new InfluxDBException(e.getMessage());
    } finally {
      ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
    }
    return new InfluxFunctionValue(count, 0L);
  }

  @Override
  public void updateValue(InfluxFunctionValue functionValue) {
    this.countNum++;
  }
}
