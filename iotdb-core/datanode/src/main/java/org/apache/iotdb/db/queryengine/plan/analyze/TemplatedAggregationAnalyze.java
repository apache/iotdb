package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.DEVICE_EXPRESSION;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.END_TIME_EXPRESSION;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeOutput;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDataPartition;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDeviceToWhere;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDeviceViewInput;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDeviceViewOutput;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeFrom;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.canPushDownLimitOffsetInGroupByTimeForDevice;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.pushDownLimitOffsetInGroupByTimeForDevice;

public class TemplatedAggregationAnalyze {

  // ----------- Methods below are used for aggregation, templated with align by device --------

  static boolean analyzeAggregation(
      Analysis analysis,
      QueryStatement queryStatement,
      IPartitionFetcher partitionFetcher,
      ISchemaTree schemaTree,
      MPPQueryContext context,
      Template template) {

    List<PartialPath> deviceList = analyzeFrom(queryStatement, schemaTree);

    if (canPushDownLimitOffsetInGroupByTimeForDevice(queryStatement)) {
      // remove the device which won't appear in resultSet after limit/offset
      deviceList = pushDownLimitOffsetInGroupByTimeForDevice(deviceList, queryStatement);
    }

    analyzeDeviceToWhere(analysis, queryStatement);
    if (deviceList.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      return true;
    }
    analysis.setDeviceList(deviceList);

    List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset());
    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {}

    analyzeSelect(queryStatement, analysis, outputExpressions, template);
    if (analysis.getWhereExpression() != null
        && analysis.getWhereExpression().equals(ConstantOperand.FALSE)) {
      analyzeOutput(analysis, queryStatement, outputExpressions);
      analysis.setFinishQueryAfterAnalyze(true);
      return true;
    }

    analyzeDeviceToAggregation(analysis);
    analyzeDeviceToSourceTransform(analysis);
    analyzeDeviceToSource(analysis);

    analyzeDeviceViewOutput(analysis, queryStatement);
    analyzeDeviceViewInput(analysis);

    // generate result set header according to output expressions
    analyzeOutput(analysis, queryStatement, outputExpressions);

    context.generateGlobalTimeFilter(analysis);
    // fetch partition information
    analyzeDataPartition(analysis, schemaTree, partitionFetcher, context.getGlobalTimeFilter());
    return true;
  }

  private static void analyzeSelect(
      QueryStatement queryStatement,
      Analysis analysis,
      List<Pair<Expression, String>> outputExpressions,
      Template template) {
    LinkedHashSet<Expression> selectExpressions = new LinkedHashSet<>();
    selectExpressions.add(DEVICE_EXPRESSION);
    if (queryStatement.isOutputEndTime()) {
      selectExpressions.add(END_TIME_EXPRESSION);
    }
    for (Pair<Expression, String> pair : outputExpressions) {
      Expression selectExpression = pair.left;
      selectExpressions.add(selectExpression);
    }
    analysis.setOutputExpressions(outputExpressions);
    analysis.setSelectExpressions(selectExpressions);
    analysis.setDeviceTemplate(template);
    // TODO only add measurement and schema occured in selectExpressions
    analysis.setMeasurementList(new ArrayList<>(template.getSchemaMap().keySet()));
    analysis.setMeasurementSchemaList(new ArrayList<>(template.getSchemaMap().values()));
  }

  private static void analyzeDeviceToSourceTransform(Analysis analysis) {
    // TODO add having into SourceTransform
    analysis.setDeviceToSourceTransformExpressions(analysis.getDeviceToSelectExpressions());
  }

  private static void analyzeDeviceToSource(Analysis analysis) {
    // TODO add having into Source
    analysis.setDeviceToSourceExpressions(analysis.getDeviceToSelectExpressions());
    analysis.setDeviceToOutputExpressions(analysis.getDeviceToSelectExpressions());
  }

  private static void analyzeDeviceToAggregation(Analysis analysis) {
    // TODO need add having clause?
    analysis.setDeviceToAggregationExpressions(analysis.getDeviceToSelectExpressions());
  }
}
