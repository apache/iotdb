package cn.edu.thu.tsfiledb.sql.exec;



import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.IllegalASTFormatException;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.*;
import cn.edu.thu.tsfiledb.qp.logical.operator.RootOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.author.AuthorOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.author.AuthorOperator.AuthorType;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.*;
import cn.edu.thu.tsfiledb.qp.logical.operator.load.LoadDataOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.metadata.MetadataOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.metadata.PropertyOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.metadata.MetadataOperator.NamespaceType;
import cn.edu.thu.tsfiledb.qp.logical.operator.metadata.PropertyOperator.PropertyType;
import cn.edu.thu.tsfiledb.sql.parse.ASTNode;
import cn.edu.thu.tsfiledb.sql.parse.Node;
import cn.edu.thu.tsfiledb.sql.parse.TSParser;

import org.antlr.runtime.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Differ from TSPlanContextV1, V2 receive a AST tree and transform it to a
 * operator, like RooTOperator and AuthorOperator
 * 
 * @author kangrong
 *
 */
public class TSPlanContextV2 {
	Logger LOG = LoggerFactory.getLogger(TSPlanContextV2.class);
	private SFWOperator rootTree = null;
	private RootOperator initialiedOperator;
	private SQLQueryType queryType;

	public SQLQueryType getQueryType() {
		return queryType;
	}

	public RootOperator getOperator() {
		return initialiedOperator;
	}

	/**
	 * input a astNode parsing by {@code anltr} and analyze it.
	 * 
	 * @param astNode
	 * @throws QueryProcessorException
	 */
	public void analyze(ASTNode astNode) throws QueryProcessorException {
		Token token = astNode.getToken();
		if (token == null)
			throw new QueryProcessorException("given token is null");
		int tokenIntType = token.getType();
		LOG.debug("analyze token: {}", token.getText());
		switch (tokenIntType) {
		case TSParser.TOK_MULTINSERT:
			analyzeMultiInsert(astNode, tokenIntType);
			return;
		case TSParser.TOK_INSERT:
			analyzeInsert(astNode, tokenIntType);
			return;
		case TSParser.TOK_QUERY:
			analyzeQuery(astNode, tokenIntType);
			break;
		case TSParser.TOK_SELECT:
			analyzeSelect(astNode);
			return;
		case TSParser.TOK_FROM:
			analyzeFrom(astNode);
			return;
		case TSParser.TOK_WHERE:
			analyzeWhere(astNode);
			return;
		case TSParser.TOK_UPDATE:
			// 这是需要修改的，因为这个update是user的 update是需要和数据的update共同使用吗？
			if (astNode.getChild(0).getType() == TSParser.TOK_UPDATE_PSWD) {
				analyzeAuthorUpdate(astNode, tokenIntType);
				return;
			}
			analyzeUpdate(astNode, tokenIntType);
			return;
		case TSParser.TOK_DELETE:
			switch (astNode.getChild(0).getType()) {
			case TSParser.TOK_TIMESERIES:
				analyzeMetadataDelete(astNode, tokenIntType);
				break;
			case TSParser.TOK_LABEL:
				analyzePropertyDeleteLabel(astNode, tokenIntType);
				break;
			default:
				analyzeDelete(astNode, tokenIntType);
				break;
			}
			return;
		case TSParser.TOK_SET:
			analyzeMetadataSetFileLevel(astNode, tokenIntType);
			return;
		case TSParser.TOK_ADD:
			analyzePropertyAddLabel(astNode, tokenIntType);
			return;
		case TSParser.TOK_LINK:
			analyzePropertyLink(astNode, tokenIntType);
			return;
		case TSParser.TOK_UNLINK:
			analyzePropertyUnLink(astNode, tokenIntType);
			return;
		case TSParser.TOK_CREATE:
			switch (astNode.getChild(0).getType()) {
			case TSParser.TOK_USER:
			case TSParser.TOK_ROLE:
				analyzeAuthorCreate(astNode, tokenIntType);
				break;
			case TSParser.TOK_TIMESERIES:
				analyzeMetadataCreate(astNode, tokenIntType);
				break;
			case TSParser.TOK_PROPERTY:
				analyzePropertyCreate(astNode, tokenIntType);
				break;
			default:
				break;
			}
			return;
		case TSParser.TOK_DROP:
			analyzeAuthorDrop(astNode, tokenIntType);
			return;
		case TSParser.TOK_GRANT:
			analyzeAuthorGrant(astNode, tokenIntType);
			return;
		case TSParser.TOK_REVOKE:
			analyzeAuthorRevoke(astNode, tokenIntType);
			return;
		case TSParser.TOK_LOAD:
			analyzeDataLoad(astNode, tokenIntType);
			return;
		default:
			// for TOK_QUERY.TOK_QUERY might appear in both query and insert
			// command. Thus, do
			// nothing and call analyze() with children nodes recursively.
			break;
		}
		for (Node node : astNode.getChildren())
			analyze((ASTNode) node);
	}

	private void analyzePropertyCreate(ASTNode astNode, int tokenIntType) {
		PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PORPERTY_CREATE,
				PropertyType.ADD_TREE);
		propertyOperator.setPropertyPath(new Path(astNode.getChild(0).getChild(0).getText()));
		initialiedOperator = propertyOperator;
	}

	private void analyzePropertyAddLabel(ASTNode astNode, int tokenIntType) {
		PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PORPERTY_ADD_LABEL,
				PropertyType.ADD_PROPERTY_LABEL);
		Path propertyLabel = parsePropertyAndLabel(astNode, 0);
		propertyOperator.setPropertyPath(propertyLabel);
		initialiedOperator = propertyOperator;
	}

	private void analyzePropertyDeleteLabel(ASTNode astNode, int tokenIntType) {
		PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PORPERTY_DELETE_LABEL,
				PropertyType.DELETE_PROPERTY_LABEL);
		Path propertyLabel = parsePropertyAndLabel(astNode, 0);
		propertyOperator.setPropertyPath(propertyLabel);
		initialiedOperator = propertyOperator;
	}

	private Path parsePropertyAndLabel(ASTNode astNode, int startIndex) {
		String label = astNode.getChild(startIndex).getChild(0).getText();
		String property = astNode.getChild(startIndex + 1).getChild(0).getText();
		return new Path(new String[] { property, label });
	}

	private void analyzePropertyLink(ASTNode astNode, int tokenIntType) {
		PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PORPERTY_LINK,
				PropertyType.ADD_PROPERTY_TO_METADATA);
		Path metaPath = parseRootPath(astNode.getChild(0));
		propertyOperator.setMetadataPath(metaPath);
		Path propertyLabel = parsePropertyAndLabel(astNode, 1);
		propertyOperator.setPropertyPath(propertyLabel);
		initialiedOperator = propertyOperator;
	}

	private void analyzePropertyUnLink(ASTNode astNode, int tokenIntType) {
		PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PORPERTY_UNLINK,
				PropertyType.DEL_PROPERTY_FROM_METADATA);
		Path metaPath = parseRootPath(astNode.getChild(0));
		propertyOperator.setMetadataPath(metaPath);
		Path propertyLabel = parsePropertyAndLabel(astNode, 1);
		propertyOperator.setPropertyPath(propertyLabel);
		initialiedOperator = propertyOperator;
	}

	private void analyzeMetadataCreate(ASTNode astNode, int tokenIntType) {
		Path series = parseRootPath(astNode.getChild(0).getChild(0));
		ASTNode paramNode = astNode.getChild(1);
		String dataType = paramNode.getChild(0).getChild(0).getText();
		String encodingType = paramNode.getChild(1).getChild(0).getText();
		String[] paramStrings = new String[paramNode.getChildCount() - 2];
		for (int i = 0; i < paramStrings.length; i++) {
			ASTNode node = paramNode.getChild(i + 2);
			paramStrings[i] = node.getChild(0) + SQLConstant.METADATA_PARAM_EQUAL + node.getChild(1);
		}
		MetadataOperator metadataOperator = new MetadataOperator(SQLConstant.TOK_METADATA_CREATE,
				NamespaceType.ADD_PATH);
		metadataOperator.setPath(series);
		metadataOperator.setDataType(dataType);
		metadataOperator.setEncoding(encodingType);
		metadataOperator.setEncodingArgs(paramStrings);
		initialiedOperator = metadataOperator;
	}

	private void analyzeMetadataDelete(ASTNode astNode, int tokenIntType) {
		Path series = parseRootPath(astNode.getChild(0).getChild(0));
		MetadataOperator metadataOperator = new MetadataOperator(SQLConstant.TOK_METADATA_DELETE,
				NamespaceType.DELETE_PATH);
		metadataOperator.setPath(series);
		initialiedOperator = metadataOperator;
	}

	private void analyzeMetadataSetFileLevel(ASTNode astNode, int tokenIntType) {
		MetadataOperator metadataOperator = new MetadataOperator(SQLConstant.TOK_METADATA_SET_FILE_LEVEL,
				NamespaceType.SET_FILE_LEVEL);
		Path path = parsePath(astNode.getChild(0).getChild(0));
		metadataOperator.setPath(path);
		initialiedOperator = metadataOperator;
	}

	public void analyzeMultiInsert(ASTNode astNode, int tokenIntType) throws QueryProcessorException{
		queryType = SQLQueryType.MULTIINSERT;
		MultiInsertOperator multiInsertOp = new MultiInsertOperator(SQLConstant.TOK_MULTIINSERT);
		initialiedOperator = rootTree = multiInsertOp;
		analyzeSelect(astNode.getChild(0));
		long timestamp;
		ASTNode timeChild = null;
		try {
			timeChild = astNode.getChild(1).getChild(0);
			if(timeChild.getToken().getType() != TSParser.TOK_TIME){
				throw new ValueParseException("need keyword 'time'");
			}
			timestamp = Long.valueOf(astNode.getChild(2).getChild(0).getText());
		} catch (NumberFormatException e) {
			throw new ValueParseException("need a long value in insert clause, but given:" + timeChild.getText());
		}
		if(astNode.getChild(1).getChildCount() != astNode.getChild(2).getChildCount()){
			throw new QueryProcessorException("length of measurement is NOT EQUAL TO the length of values");
		}
		multiInsertOp.setInsertTime(timestamp);
		List<String> measurementList = new ArrayList<String>();
		for(int i = 1; i < astNode.getChild(1).getChildCount(); i ++){
			measurementList.add(astNode.getChild(1).getChild(i).getText());
		}
		multiInsertOp.setMeasurementList(measurementList);
		
		List<String> insertValue = new ArrayList<String>();
		for(int i = 1; i < astNode.getChild(2).getChildCount(); i ++){
			insertValue.add(astNode.getChild(2).getChild(i).getText());
		}
		multiInsertOp.setInsertValue(insertValue);
	}
	
	/**
	 * @param astNode
	 * @param tokenIntType
	 */
	private void analyzeInsert(ASTNode astNode, int tokenIntType) throws QueryProcessorException {
		queryType = SQLQueryType.INSERT;
		InsertOperator insertOp = new InsertOperator(SQLConstant.TOK_INSERT);
		initialiedOperator = rootTree = insertOp;
		analyzeSelect(astNode.getChild(0));
		long timestamp;
		ASTNode timeChild = null;
		try {
			timeChild = astNode.getChild(1);
			timestamp = Long.valueOf(timeChild.getText());
		} catch (NumberFormatException e) {
			throw new ValueParseException("need a long value in insert clause, but given:" + timeChild.getText());
		}
		insertOp.setInsertTime(timestamp);
		insertOp.setInsertValue(astNode.getChild(2).getText());
	}

	/**
	 * @param astNode
	 * @param tokenIntType
	 */
	private void analyzeUpdate(ASTNode astNode, int tokenIntType) throws QueryProcessorException {

		System.out.println("you are reaaly here");
		ASTNode next = astNode.getChild(0);
		System.out.println(next.toString());

		// code above is editted by zhaoxin ,2017-1-4

		if (astNode.getChildCount() != 3)
			throw new UpdateOperatorException("error format in UPDATE statement:" + astNode.dump());
		queryType = SQLQueryType.UPDATE;
		UpdateOperator updateOp = new UpdateOperator(SQLConstant.TOK_UPDATE);
		initialiedOperator = rootTree = updateOp;
		analyzeSelect(astNode.getChild(0));
		if (astNode.getChild(1).getType() != TSParser.TOK_VALUE)
			throw new UpdateOperatorException("error format in UPDATE statement:" + astNode.dump());
		updateOp.setUpdateValue(astNode.getChild(1).getChild(0).getText());
		analyzeWhere(astNode.getChild(2));
	}

	/**
	 * @param astNode
	 * @param tokenIntType
	 */
	private void analyzeDelete(ASTNode astNode, int tokenIntType) throws QueryProcessorException {
		if (astNode.getChildCount() != 2)
			throw new UpdateOperatorException("error format in DELETE statement:" + astNode.dump());
		queryType = SQLQueryType.DELETE;
		DeleteOperator deleteOp = new DeleteOperator(SQLConstant.TOK_DELETE);
		initialiedOperator = rootTree = deleteOp;
		analyzeSelect(astNode.getChild(0));
		analyzeWhere(astNode.getChild(1));
	}

	/**
	 * @param astNode
	 * @param tokenIntType
	 */
	private void analyzeQuery(ASTNode astNode, int tokenIntType) throws QueryProcessorException {
		queryType = SQLQueryType.QUERY;
		QueryOperator queryOp = new QueryOperator(SQLConstant.TOK_QUERY);
		initialiedOperator = rootTree = queryOp;
	}

	/**
	 * 
	 * up to now(2016-09-23), Antlr, TOK_TABNAME meaning FROM in Query command
	 * only appear below TOK_TABREF, other TOK_TABNAME in update/insert/delete
	 * means select series path and don't pass this route
	 * 
	 * @param node
	 * @return
	 */
	private void analyzeFrom(ASTNode node) throws QpSelectFromException {
		if (node.getType() != TSParser.TOK_FROM)
			throw new QpSelectFromException("parse from filter in query failed,meet token:" + node.getText());
		int selChildCount = node.getChildCount();
		FromOperator from = new FromOperator(SQLConstant.TOK_FROM);
		for (int i = 0; i < selChildCount; i++) {
			ASTNode child = node.getChild(i);
			if (child.getType() != TSParser.TOK_PATH) {
				throw new QpSelectFromException("children FROM clause must all be TOK_PATH, actual:" + child.getText());
			}
			Path tablePath = parsePath(child);
			from.addPrefixTablePath(tablePath);
		}
		this.rootTree.setFromOperator(from);
	}

	private void analyzeSelect(ASTNode astNode) throws QpSelectFromException {
		int tokenIntType = astNode.getType();
		SelectOperator selectOp = new SelectOperator(TSParser.TOK_SELECT);
		if (tokenIntType == TSParser.TOK_SELECT) {
			int selChildCount = astNode.getChildCount();
			for (int i = 0; i < selChildCount; i++) {
				ASTNode child = astNode.getChild(i);
				switch (child.getType()) {
				case TSParser.TOK_CLUSTER:
					ASTNode pathChild = child.getChild(0);
					Path selectPath = parsePath(pathChild);
					String aggregation = child.getChild(1).getText();
					selectOp.addSuffixTablePath(selectPath, aggregation);
				case TSParser.TOK_PATH:
					selectPath = parsePath(child);
					selectOp.addSuffixTablePath(selectPath);
					break;

				default:
					throw new QpSelectFromException(
							"children SELECT clause must all be TOK_PATH, actual:" + tokenIntType);
				}

			}
		} else if (tokenIntType == TSParser.TOK_PATH) {
			Path selectPath = parsePath(astNode);
			selectOp.addSuffixTablePath(selectPath);
		} else
			throw new QpSelectFromException("children SELECT clause must all be TOK_PATH, actual:" + astNode.dump());
		this.rootTree.setSelectOperator(selectOp);
	}

	private void analyzeWhere(ASTNode astNode) throws QpWhereException, BasicOperatorException {
		if (astNode.getType() != TSParser.TOK_WHERE)
			throw new QpWhereException("given node is not WHERE! " + astNode.dump());
		if (astNode.getChildCount() != 1)
			throw new QpWhereException("where clause has {} child, return" + astNode.getChildCount());
		FilterOperator whereOp = new FilterOperator(SQLConstant.TOK_WHERE);
		ASTNode child = astNode.getChild(0);
		analyzeWhere(child, child.getType(), whereOp);
		this.rootTree.setFilterOperator(whereOp.getChildren().get(0));
	}

	private void analyzeWhere(ASTNode ast, int tokenIntType, FilterOperator filterOp)
			throws QpWhereException, BasicOperatorException {
		int childCount = ast.getChildCount();
		switch (tokenIntType) {
		case TSParser.KW_NOT:
			if (childCount != 1) {
				throw new QpWhereException("parsing where clause failed: children count != 1");
			}
			FilterOperator notOp = new FilterOperator(SQLConstant.KW_NOT);
			filterOp.addChildOPerator(notOp);
			ASTNode childAstNode = ast.getChild(0);
			int childNodeTokenType = childAstNode.getToken().getType();
			analyzeWhere(childAstNode, childNodeTokenType, notOp);
			break;
		case TSParser.KW_AND:
		case TSParser.KW_OR:
			if (childCount != 2) {
				throw new QpWhereException("parsing where clause failed! node has " + childCount + " paramter.");
			}
			FilterOperator binaryOp = new FilterOperator(TSParserConstant.getTSTokenIntType(tokenIntType));
			filterOp.addChildOPerator(binaryOp);
			for (int i = 0; i < childCount; i++) {
				childAstNode = ast.getChild(i);
				childNodeTokenType = childAstNode.getToken().getType();
				analyzeWhere(childAstNode, childNodeTokenType, binaryOp);
			}
			break;
		case TSParser.LESSTHAN:
		case TSParser.LESSTHANOREQUALTO:
		case TSParser.EQUAL:
		case TSParser.EQUAL_NS:
		case TSParser.GREATERTHAN:
		case TSParser.GREATERTHANOREQUALTO:
		case TSParser.NOTEQUAL:
			Pair<Path, String> pair = parseLeafNode(ast);
			// if (pair == null)
			// new
			// FilterOperator(TSParserConstant.anltrQpMap.get(tokenIntType));
			BasicFunctionOperator basic = new BasicFunctionOperator(TSParserConstant.getTSTokenIntType(tokenIntType),
					pair.left, pair.right);
			filterOp.addChildOPerator(basic);
			break;
		default:
			throw new QpWhereException("unsupported token:" + tokenIntType);
		}
	}

	private Pair<Path, String> parseLeafNode(ASTNode node) throws ParseLeafException {
		if (node.getChildCount() != 2)
			throw new ParseLeafException("leaf node children count != 2");
		ASTNode col = node.getChild(0);
		if (col.getType() != TSParser.TOK_PATH)
			throw new ParseLeafException("left node of leaf isn't PATH");
		Path seriesPath = parsePath(col);
		// String seriesValue = node.getChild(1).getChild(0).getText();
		ASTNode rightKey = node.getChild(1);
		String seriesValue;
		if (rightKey.getType() == TSParser.TOK_PATH)
			seriesValue = parsePath(rightKey).getFullPath();
		else if (rightKey.getType() == TSParser.TOK_DATETIME) {
			seriesValue = parseTokenTime(rightKey);
		} else
			seriesValue = rightKey.getText();
		return new Pair<Path, String>(seriesPath, seriesValue);
	}

	protected String parseTokenTime(ASTNode astNode) throws ParseLeafException {
		SimpleDateFormat sdf;
		sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");

		StringContainer sc = new StringContainer();
		for (int i = 0; i < astNode.getChildCount(); i++) {
			sc.addTail(astNode.getChild(i).getText());
		}
		Date date = new Date();
		try {
			date = sdf.parse(sc.toString());
		} catch (ParseException e) {
			throw new ParseLeafException("parse time error,String:" + sc.toString() + "message:" + e.getMessage());
		}
		return String.valueOf(date.getTime());
	}

	private Path parsePath(ASTNode node) {
		int childCount = node.getChildCount();
		String[] path = new String[node.getChildCount()];
		for (int i = 0; i < childCount; i++) {
			path[i] = node.getChild(i).getText().toLowerCase();
		}
		return new Path(new StringContainer(path, SQLConstant.PATH_SEPARATOR));
	}

	private Path parseRootPath(ASTNode node) {
		StringContainer sc = new StringContainer(SQLConstant.PATH_SEPARATOR);
		sc.addTail(SQLConstant.ROOT);
		int childCount = node.getChildCount();
		for (int i = 0; i < childCount; i++) {
			sc.addTail(node.getChild(i).getText().toLowerCase());
		}
		return new Path(sc);
	}

	private String parseStringWithQuoto(String src) throws IllegalASTFormatException {
		if (src.length() < 3 || src.charAt(0) != '\'' || src.charAt(src.length() - 1) != '\'')
			throw new IllegalASTFormatException("error format for string with quoto:" + src);
		return src.substring(1, src.length() - 1);
	}

	/**
	 * return a parameter string separated by space acquiescently.
	 * 
	 * @return
	 */
	// public PhysicalPlan thansformToPhysicalPlan(QueryProcessExecutor conf) {
	// return this.rootTree.transformToPhysicalPlan(conf);
	// }

	private void analyzeDataLoad(ASTNode astNode, int tokenIntType) throws IllegalASTFormatException {
		int childCount = astNode.getChildCount();
		// node path should have more than one level and first node level must
		// be root
		if (childCount < 3 || !SQLConstant.ROOT.equals(astNode.getChild(1).getText().toLowerCase()))
			throw new IllegalASTFormatException("data load command: child count < 3\n" + astNode.dump());
		String csvPath = astNode.getChild(0).getText();
		if (csvPath.length() < 3 || csvPath.charAt(0) != '\'' || csvPath.charAt(csvPath.length() - 1) != '\'')
			throw new IllegalASTFormatException("data load: error format csvPath:" + csvPath);
		StringContainer sc = new StringContainer(SQLConstant.PATH_SEPARATOR);
		sc.addTail(SQLConstant.ROOT);
		for (int i = 2; i < childCount; i++) {
			String pathNode = astNode.getChild(i).getText().toLowerCase();
			sc.addTail(pathNode);
		}
		initialiedOperator = new LoadDataOperator(SQLConstant.TOK_DATALOAD, csvPath.substring(1, csvPath.length() - 1),
				sc.toString());
	}

	private void analyzeAuthorCreate(ASTNode astNode, int tokenIntType) throws IllegalASTFormatException {
		int childCount = astNode.getChildCount();
		AuthorOperator authorOperator = null;
		if (childCount == 2) {
			// create user
			authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_CREATE, AuthorType.CREATE_USER);
			authorOperator.setUserName(astNode.getChild(0).getChild(0).getText());
			authorOperator.setPassWord(astNode.getChild(1).getChild(0).getText());
		} else if (childCount == 1) {
			// create role
			authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_CREATE, AuthorType.CREATE_ROLE);
			authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
		} else {
			throw new IllegalASTFormatException("illegal ast tree in create author command:\n" + astNode.dump());
		}
		initialiedOperator = authorOperator;
	}

	private void analyzeAuthorUpdate(ASTNode astNode, int tokenIntType) throws IllegalASTFormatException {
		int childCount = astNode.getChildCount();
		AuthorOperator authorOperator = null;
		if (childCount == 1) {
			// 构造对应的 update user的 operator
			authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_UPDATE_USER, AuthorType.UPDATE_USER);
			// 设置对应的参数
			// 设置username
			// 设置old password
			// 设置new password
			ASTNode user = astNode.getChild(0);
			if (user.getChildCount() != 3) {
				throw new IllegalASTFormatException("illegal ast tree in create author command:\n" + astNode.dump());
			}
			System.out.println(user.dump());
			authorOperator.setUserName(parseStringWithQuoto(user.getChild(0).getText()));
			// authorOperator.setPassWord(parseStringWithQuoto(user.getChild(1).getText()));
			authorOperator.setNewPassword(parseStringWithQuoto(user.getChild(1).getText()));
		} else {
			throw new IllegalASTFormatException("illegal ast tree in create author command:\n" + astNode.dump());
		}
		initialiedOperator = authorOperator;
	}

	private void analyzeAuthorDrop(ASTNode astNode, int tokenIntType) throws IllegalASTFormatException {
		int childCount = astNode.getChildCount();
		AuthorOperator authorOperator = null;
		if (childCount == 1) {
			// drop user or role
			switch (astNode.getChild(0).getType()) {
			case TSParser.TOK_USER:
				authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_DROP, AuthorType.DROP_USER);
				authorOperator.setUserName(astNode.getChild(0).getChild(0).getText());
				break;
			case TSParser.TOK_ROLE:
				authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_DROP, AuthorType.DROP_ROLE);
				authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
				break;
			default:
				throw new IllegalASTFormatException("illegal ast tree in drop author command:\n" + astNode.dump());
			}
			;
		} else {
			throw new IllegalASTFormatException("illegal ast tree in drop author command:\n" + astNode.dump());
		}
		initialiedOperator = authorOperator;
	}

	private void analyzeAuthorGrant(ASTNode astNode, int tokenIntType) throws IllegalASTFormatException {
		int childCount = astNode.getChildCount();
		AuthorOperator authorOperator = null;
		if (childCount == 2) {
			// grant role to user
			authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT, AuthorType.GRANT_ROLE_TO_USER);
			authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
			authorOperator.setUserName(astNode.getChild(1).getChild(0).getText());
		} else if (childCount == 3) {
			ASTNode privilegesNode = astNode.getChild(1);
			String[] privileges = new String[privilegesNode.getChildCount()];
			for (int i = 0; i < privileges.length; i++) {
				privileges[i] = parseStringWithQuoto(privilegesNode.getChild(i).getText());
			}
			ASTNode pathNode = astNode.getChild(2);
			String[] nodeNameList = new String[pathNode.getChildCount()];
			for (int i = 0; i < nodeNameList.length; i++) {
				nodeNameList[i] = pathNode.getChild(i).getText();
			}
			if (astNode.getChild(0).getType() == TSParser.TOK_USER) {
				// grant user
				authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT, AuthorType.GRANT_USER);
				authorOperator.setUserName(astNode.getChild(0).getChild(0).getText());
				authorOperator.setPrivilegeList(privileges);
				authorOperator.setNodeNameList(nodeNameList);
			} else if (astNode.getChild(0).getType() == TSParser.TOK_ROLE) {
				// grant role
				authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT, AuthorType.GRANT_ROLE);
				authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
				authorOperator.setPrivilegeList(privileges);
				authorOperator.setNodeNameList(nodeNameList);
			} else {
				throw new IllegalASTFormatException("illegal ast tree in grant author command:\n" + astNode.dump());
			}
		} else {
			throw new IllegalASTFormatException("illegal ast tree in grant author command:\n" + astNode.dump());
		}
		initialiedOperator = authorOperator;
	}

	private void analyzeAuthorRevoke(ASTNode astNode, int tokenIntType) throws IllegalASTFormatException {
		int childCount = astNode.getChildCount();
		AuthorOperator authorOperator = null;
		if (childCount == 2) {
			// revoke role to user
			authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_REVOKE, AuthorType.REVOKE_ROLE_FROM_USER);
			authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
			authorOperator.setUserName(astNode.getChild(1).getChild(0).getText());
		} else if (childCount == 3) {
			ASTNode privilegesNode = astNode.getChild(1);
			String[] privileges = new String[privilegesNode.getChildCount()];
			for (int i = 0; i < privileges.length; i++) {
				privileges[i] = parseStringWithQuoto(privilegesNode.getChild(i).getText());
			}
			ASTNode pathNode = astNode.getChild(2);
			String[] nodeNameList = new String[pathNode.getChildCount()];
			for (int i = 0; i < nodeNameList.length; i++) {
				nodeNameList[i] = pathNode.getChild(i).getText();
			}
			if (astNode.getChild(0).getType() == TSParser.TOK_USER) {
				// grant user
				authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_REVOKE, AuthorType.REVOKE_USER);
				authorOperator.setUserName(astNode.getChild(0).getChild(0).getText());
				authorOperator.setPrivilegeList(privileges);
				authorOperator.setNodeNameList(nodeNameList);
			} else if (astNode.getChild(0).getType() == TSParser.TOK_ROLE) {
				// grant role
				authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_REVOKE, AuthorType.REVOKE_ROLE);
				authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
				authorOperator.setPrivilegeList(privileges);
				authorOperator.setNodeNameList(nodeNameList);
			} else {
				throw new IllegalASTFormatException("illegal ast tree in revoke author command:\n" + astNode.dump());
			}
		} else {
			throw new IllegalASTFormatException("illegal ast tree in revoke author command:\n" + astNode.dump());
		}
		initialiedOperator = authorOperator;
	}

	public enum SQLQueryType {
		MULTIINSERT, INSERT, DELETE, UPDATE, QUERY
	}

}
