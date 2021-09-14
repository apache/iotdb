// Generated from /Users/jun/IdeaProjects/iotdb-influxdb/src/main/java/org/apache/iotdb/infludb/antlr/InfluxDB.g4 by ANTLR 4.9.1
package org.apache.iotdb.infludb.gen;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link InfluxDBParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface InfluxDBVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#singleStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleStatement(InfluxDBParser.SingleStatementContext ctx);
	/**
	 * Visit a parse tree produced by the {@code selectStatement}
	 * labeled alternative in {@link InfluxDBParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectStatement(InfluxDBParser.SelectStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#selectClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectClause(InfluxDBParser.SelectClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#resultColumn}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitResultColumn(InfluxDBParser.ResultColumnContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(InfluxDBParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#functionAttribute}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionAttribute(InfluxDBParser.FunctionAttributeContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#compressor}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCompressor(InfluxDBParser.CompressorContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#whereClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhereClause(InfluxDBParser.WhereClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#orExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrExpression(InfluxDBParser.OrExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#andExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAndExpression(InfluxDBParser.AndExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicate(InfluxDBParser.PredicateContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#fromClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFromClause(InfluxDBParser.FromClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#comparisonOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparisonOperator(InfluxDBParser.ComparisonOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#propertyValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPropertyValue(InfluxDBParser.PropertyValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#nodeName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNodeName(InfluxDBParser.NodeNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#nodeNameWithoutStar}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNodeNameWithoutStar(InfluxDBParser.NodeNameWithoutStarContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#dataType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDataType(InfluxDBParser.DataTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#dateFormat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDateFormat(InfluxDBParser.DateFormatContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstant(InfluxDBParser.ConstantContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#booleanClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanClause(InfluxDBParser.BooleanClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#dateExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDateExpression(InfluxDBParser.DateExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#encoding}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEncoding(InfluxDBParser.EncodingContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#realLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRealLiteral(InfluxDBParser.RealLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link InfluxDBParser#stringLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringLiteral(InfluxDBParser.StringLiteralContext ctx);
}