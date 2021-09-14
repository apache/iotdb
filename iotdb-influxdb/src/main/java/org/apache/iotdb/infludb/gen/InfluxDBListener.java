// Generated from /Users/jun/IdeaProjects/iotdb-influxdb/src/main/java/org/apache/iotdb/infludb/antlr/InfluxDB.g4 by ANTLR 4.9.1
package org.apache.iotdb.infludb.gen;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link InfluxDBParser}.
 */
public interface InfluxDBListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void enterSingleStatement(InfluxDBParser.SingleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void exitSingleStatement(InfluxDBParser.SingleStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code selectStatement}
	 * labeled alternative in {@link InfluxDBParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSelectStatement(InfluxDBParser.SelectStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code selectStatement}
	 * labeled alternative in {@link InfluxDBParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSelectStatement(InfluxDBParser.SelectStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void enterSelectClause(InfluxDBParser.SelectClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void exitSelectClause(InfluxDBParser.SelectClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#resultColumn}.
	 * @param ctx the parse tree
	 */
	void enterResultColumn(InfluxDBParser.ResultColumnContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#resultColumn}.
	 * @param ctx the parse tree
	 */
	void exitResultColumn(InfluxDBParser.ResultColumnContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(InfluxDBParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(InfluxDBParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#functionAttribute}.
	 * @param ctx the parse tree
	 */
	void enterFunctionAttribute(InfluxDBParser.FunctionAttributeContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#functionAttribute}.
	 * @param ctx the parse tree
	 */
	void exitFunctionAttribute(InfluxDBParser.FunctionAttributeContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#compressor}.
	 * @param ctx the parse tree
	 */
	void enterCompressor(InfluxDBParser.CompressorContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#compressor}.
	 * @param ctx the parse tree
	 */
	void exitCompressor(InfluxDBParser.CompressorContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void enterWhereClause(InfluxDBParser.WhereClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void exitWhereClause(InfluxDBParser.WhereClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#orExpression}.
	 * @param ctx the parse tree
	 */
	void enterOrExpression(InfluxDBParser.OrExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#orExpression}.
	 * @param ctx the parse tree
	 */
	void exitOrExpression(InfluxDBParser.OrExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#andExpression}.
	 * @param ctx the parse tree
	 */
	void enterAndExpression(InfluxDBParser.AndExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#andExpression}.
	 * @param ctx the parse tree
	 */
	void exitAndExpression(InfluxDBParser.AndExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterPredicate(InfluxDBParser.PredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitPredicate(InfluxDBParser.PredicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void enterFromClause(InfluxDBParser.FromClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void exitFromClause(InfluxDBParser.FromClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void enterComparisonOperator(InfluxDBParser.ComparisonOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void exitComparisonOperator(InfluxDBParser.ComparisonOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	void enterPropertyValue(InfluxDBParser.PropertyValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	void exitPropertyValue(InfluxDBParser.PropertyValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#nodeName}.
	 * @param ctx the parse tree
	 */
	void enterNodeName(InfluxDBParser.NodeNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#nodeName}.
	 * @param ctx the parse tree
	 */
	void exitNodeName(InfluxDBParser.NodeNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#nodeNameWithoutStar}.
	 * @param ctx the parse tree
	 */
	void enterNodeNameWithoutStar(InfluxDBParser.NodeNameWithoutStarContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#nodeNameWithoutStar}.
	 * @param ctx the parse tree
	 */
	void exitNodeNameWithoutStar(InfluxDBParser.NodeNameWithoutStarContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterDataType(InfluxDBParser.DataTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitDataType(InfluxDBParser.DataTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#dateFormat}.
	 * @param ctx the parse tree
	 */
	void enterDateFormat(InfluxDBParser.DateFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#dateFormat}.
	 * @param ctx the parse tree
	 */
	void exitDateFormat(InfluxDBParser.DateFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterConstant(InfluxDBParser.ConstantContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitConstant(InfluxDBParser.ConstantContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#booleanClause}.
	 * @param ctx the parse tree
	 */
	void enterBooleanClause(InfluxDBParser.BooleanClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#booleanClause}.
	 * @param ctx the parse tree
	 */
	void exitBooleanClause(InfluxDBParser.BooleanClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#dateExpression}.
	 * @param ctx the parse tree
	 */
	void enterDateExpression(InfluxDBParser.DateExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#dateExpression}.
	 * @param ctx the parse tree
	 */
	void exitDateExpression(InfluxDBParser.DateExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#encoding}.
	 * @param ctx the parse tree
	 */
	void enterEncoding(InfluxDBParser.EncodingContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#encoding}.
	 * @param ctx the parse tree
	 */
	void exitEncoding(InfluxDBParser.EncodingContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#realLiteral}.
	 * @param ctx the parse tree
	 */
	void enterRealLiteral(InfluxDBParser.RealLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#realLiteral}.
	 * @param ctx the parse tree
	 */
	void exitRealLiteral(InfluxDBParser.RealLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link InfluxDBParser#stringLiteral}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(InfluxDBParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link InfluxDBParser#stringLiteral}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(InfluxDBParser.StringLiteralContext ctx);
}