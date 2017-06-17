package cn.edu.thu.tsfiledb.qp.strategy;

import cn.edu.thu.tsfile.common.constant.SystemConstant;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.constant.TSParserConstant;
import cn.edu.thu.tsfiledb.qp.exception.*;
import cn.edu.thu.tsfiledb.qp.logical.RootOperator;
import cn.edu.thu.tsfiledb.qp.logical.crud.*;
import cn.edu.thu.tsfiledb.qp.logical.sys.AuthorOperator;
import cn.edu.thu.tsfiledb.qp.logical.sys.AuthorOperator.AuthorType;
import cn.edu.thu.tsfiledb.qp.logical.sys.LoadDataOperator;
import cn.edu.thu.tsfiledb.qp.logical.sys.MetadataOperator;
import cn.edu.thu.tsfiledb.qp.logical.sys.PropertyOperator;
import cn.edu.thu.tsfiledb.qp.logical.sys.MetadataOperator.NamespaceType;
import cn.edu.thu.tsfiledb.qp.logical.sys.PropertyOperator.PropertyType;
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

import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.*;

/**
 * This class receives an ASTNode and transform it to an operator which is a logical plan
 *
 * @author kangrong
 * @author qiaojialin
 *
 */
public class LogicalGenerator {
    private Logger LOG = LoggerFactory.getLogger(LogicalGenerator.class);
    private RootOperator initializedOperator = null;

    public RootOperator getLogicalPlan(ASTNode astNode) throws QueryProcessorException {
        analyze(astNode);
        return initializedOperator;
    }

    /**
     * input an astNode parsing by {@code antlr} and analyze it.
     *
     */
    private void analyze(ASTNode astNode) throws QueryProcessorException {
        Token token = astNode.getToken();
        if (token == null)
            throw new QueryProcessorException("given token is null");
        int tokenIntType = token.getType();
        LOG.debug("analyze token: {}", token.getText());
        switch (tokenIntType) {
            case TSParser.TOK_MULTINSERT:
                analyzeMultiInsert(astNode);
                return;
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
                if (astNode.getChild(0).getType() == TSParser.TOK_UPDATE_PSWD) {
                    analyzeAuthorUpdate(astNode);
                    return;
                }
                analyzeUpdate(astNode);
                return;
            case TSParser.TOK_DELETE:
                switch (astNode.getChild(0).getType()) {
                    case TSParser.TOK_TIMESERIES:
                        analyzeMetadataDelete(astNode);
                        break;
                    case TSParser.TOK_LABEL:
                        analyzePropertyDeleteLabel(astNode);
                        break;
                    default:
                        analyzeDelete(astNode);
                        break;
                }
                return;
            case TSParser.TOK_SET:
                analyzeMetadataSetFileLevel(astNode);
                return;
            case TSParser.TOK_ADD:
                analyzePropertyAddLabel(astNode);
                return;
            case TSParser.TOK_LINK:
                analyzePropertyLink(astNode);
                return;
            case TSParser.TOK_UNLINK:
                analyzePropertyUnLink(astNode);
                return;
            case TSParser.TOK_CREATE:
                switch (astNode.getChild(0).getType()) {
                    case TSParser.TOK_USER:
                    case TSParser.TOK_ROLE:
                        analyzeAuthorCreate(astNode);
                        break;
                    case TSParser.TOK_TIMESERIES:
                        analyzeMetadataCreate(astNode);
                        break;
                    case TSParser.TOK_PROPERTY:
                        analyzePropertyCreate(astNode);
                        break;
                    default:
                        break;
                }
                return;
            case TSParser.TOK_DROP:
                analyzeAuthorDrop(astNode);
                return;
            case TSParser.TOK_GRANT:
                analyzeAuthorGrant(astNode);
                return;
            case TSParser.TOK_REVOKE:
                analyzeAuthorRevoke(astNode);
                return;
            case TSParser.TOK_LOAD:
                analyzeDataLoad(astNode);
                return;
            case TSParser.TOK_QUERY:
                // for TSParser.TOK_QUERY might appear in both query and insert
                // command. Thus, do
                // nothing and call analyze() with children nodes recursively.
                initializedOperator = new QueryOperator(SQLConstant.TOK_QUERY);
                break;
            default:
                throw new QueryProcessorException("Not supported TSParser type" + tokenIntType);
        }
        for (Node node : astNode.getChildren())
            analyze((ASTNode) node);
    }

    private void analyzePropertyCreate(ASTNode astNode) {
        PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_CREATE,
                PropertyType.ADD_TREE);
        propertyOperator.setPropertyPath(new Path(astNode.getChild(0).getChild(0).getText()));
        initializedOperator = propertyOperator;
    }

    private void analyzePropertyAddLabel(ASTNode astNode) {
        PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_ADD_LABEL,
                PropertyType.ADD_PROPERTY_LABEL);
        Path propertyLabel = parsePropertyAndLabel(astNode, 0);
        propertyOperator.setPropertyPath(propertyLabel);
        initializedOperator = propertyOperator;
    }

    private void analyzePropertyDeleteLabel(ASTNode astNode) {
        PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_DELETE_LABEL,
                PropertyType.DELETE_PROPERTY_LABEL);
        Path propertyLabel = parsePropertyAndLabel(astNode, 0);
        propertyOperator.setPropertyPath(propertyLabel);
        initializedOperator = propertyOperator;
    }

    private Path parsePropertyAndLabel(ASTNode astNode, int startIndex) {
        String label = astNode.getChild(startIndex).getChild(0).getText();
        String property = astNode.getChild(startIndex + 1).getChild(0).getText();
        return new Path(new String[]{property, label});
    }

    private void analyzePropertyLink(ASTNode astNode) {
        PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_LINK,
                PropertyType.ADD_PROPERTY_TO_METADATA);
        Path metaPath = parseRootPath(astNode.getChild(0));
        propertyOperator.setMetadataPath(metaPath);
        Path propertyLabel = parsePropertyAndLabel(astNode, 1);
        propertyOperator.setPropertyPath(propertyLabel);
        initializedOperator = propertyOperator;
    }

    private void analyzePropertyUnLink(ASTNode astNode) {
        PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_UNLINK,
                PropertyType.DEL_PROPERTY_FROM_METADATA);
        Path metaPath = parseRootPath(astNode.getChild(0));
        propertyOperator.setMetadataPath(metaPath);
        Path propertyLabel = parsePropertyAndLabel(astNode, 1);
        propertyOperator.setPropertyPath(propertyLabel);
        initializedOperator = propertyOperator;
    }

    private void analyzeMetadataCreate(ASTNode astNode) {
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
        initializedOperator = metadataOperator;
    }

    private void analyzeMetadataDelete(ASTNode astNode) {
        Path series = parseRootPath(astNode.getChild(0).getChild(0));
        MetadataOperator metadataOperator = new MetadataOperator(SQLConstant.TOK_METADATA_DELETE,
                NamespaceType.DELETE_PATH);
        metadataOperator.setPath(series);
        initializedOperator = metadataOperator;
    }

    private void analyzeMetadataSetFileLevel(ASTNode astNode) {
        MetadataOperator metadataOperator = new MetadataOperator(SQLConstant.TOK_METADATA_SET_FILE_LEVEL,
                NamespaceType.SET_FILE_LEVEL);
        Path path = parsePath(astNode.getChild(0).getChild(0));
        metadataOperator.setPath(path);
        initializedOperator = metadataOperator;
    }

    private void analyzeMultiInsert(ASTNode astNode) throws QueryProcessorException {
        MultiInsertOperator multiInsertOp = new MultiInsertOperator(SQLConstant.TOK_MULTIINSERT);
        initializedOperator = multiInsertOp;
        analyzeSelect(astNode.getChild(0));
        long timestamp;
        ASTNode timeChild;
        try {
            timeChild = astNode.getChild(1).getChild(0);
            if (timeChild.getToken().getType() != TSParser.TOK_TIME) {
                throw new LogicalOperatorException("need keyword 'timestamp'");
            }
            timestamp = Long.valueOf(astNode.getChild(2).getChild(0).getText());
        } catch (NumberFormatException e) {
            throw new LogicalOperatorException("need a long value in insert clause, but given:" + astNode.getChild(2).getChild(0).getText());
        }
        if (astNode.getChild(1).getChildCount() != astNode.getChild(2).getChildCount()) {
            throw new QueryProcessorException("length of measurement is NOT EQUAL TO the length of values");
        }
        multiInsertOp.setTime(timestamp);
        List<String> measurementList = new ArrayList<>();
        for (int i = 1; i < astNode.getChild(1).getChildCount(); i++) {
            measurementList.add(astNode.getChild(1).getChild(i).getText());
        }
        multiInsertOp.setMeasurementList(measurementList);

        List<String> valueList = new ArrayList<>();
        for (int i = 1; i < astNode.getChild(2).getChildCount(); i++) {
            valueList.add(astNode.getChild(2).getChild(i).getText());
        }
        multiInsertOp.setValueList(valueList);
    }


    private void analyzeUpdate(ASTNode astNode) throws QueryProcessorException {
        if (astNode.getChildCount() != 3)
            throw new LogicalOperatorException("error format in UPDATE statement:" + astNode.dump());
        UpdateOperator updateOp = new UpdateOperator(SQLConstant.TOK_UPDATE);
        initializedOperator = updateOp;
        analyzeSelect(astNode.getChild(0));
        if (astNode.getChild(1).getType() != TSParser.TOK_VALUE)
            throw new LogicalOperatorException("error format in UPDATE statement:" + astNode.dump());
        updateOp.setValue(astNode.getChild(1).getChild(0).getText());
        analyzeWhere(astNode.getChild(2));
    }


    private void analyzeDelete(ASTNode astNode) throws LogicalOperatorException {
        if (astNode.getChildCount() != 2)
            throw new LogicalOperatorException("error format in DELETE statement:" + astNode.dump());
        initializedOperator = new DeleteOperator(SQLConstant.TOK_DELETE);
        analyzeSelect(astNode.getChild(0));
        analyzeWhere(astNode.getChild(1));
        long deleteTime = parseDeleteTimeFilter((DeleteOperator) initializedOperator);
        ((DeleteOperator) initializedOperator).setTime(deleteTime);
    }

    /**
     * for delete command, time should only have an end time.
     *
     * @param operator delete logical plan
     */
    private long parseDeleteTimeFilter(DeleteOperator operator) throws LogicalOperatorException {
        FilterOperator filterOperator= operator.getFilterOperator();
        if (!(filterOperator.isLeaf())) {
            throw new LogicalOperatorException(
                    "for delete command, where clause must be like : time < XXX or time <= XXX");
        }

        if (filterOperator.getTokenIntType() != LESSTHAN
                && filterOperator.getTokenIntType() != LESSTHANOREQUALTO) {
            throw new LogicalOperatorException(
                    "for delete command, time filter must be less than or less than or equal to, this:"
                            + filterOperator.getTokenIntType());
        }
        long time = Long.valueOf(((BasicFunctionOperator) filterOperator).getValue());

        if (time < 0) {
            throw new LogicalOperatorException("delete Time:" + time + ", time must >= 0");
        }
        return time;
    }


    private void analyzeFrom(ASTNode node) throws LogicalOperatorException {
        int selChildCount = node.getChildCount();
        FromOperator from = new FromOperator(SQLConstant.TOK_FROM);
        for (int i = 0; i < selChildCount; i++) {
            ASTNode child = node.getChild(i);
            if (child.getType() != TSParser.TOK_PATH) {
                throw new LogicalOperatorException("children FROM clause must all be TOK_PATH, actual:" + child.getText());
            }
            Path tablePath = parsePath(child);
            from.addPrefixTablePath(tablePath);
        }
        ((SFWOperator) initializedOperator).setFromOperator(from);
    }

    private void analyzeSelect(ASTNode astNode) throws LogicalOperatorException {
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
                        throw new LogicalOperatorException(
                                "children SELECT clause must all be TOK_PATH, actual:" + tokenIntType);
                }
            }
        } else if (tokenIntType == TSParser.TOK_PATH) {
            Path selectPath = parsePath(astNode);
            selectOp.addSuffixTablePath(selectPath);
        } else
            throw new LogicalOperatorException("children SELECT clause must all be TOK_PATH, actual:" + astNode.dump());
        ((SFWOperator) initializedOperator).setSelectOperator(selectOp);
    }

    private void analyzeWhere(ASTNode astNode) throws LogicalOperatorException {
        if (astNode.getType() != TSParser.TOK_WHERE)
            throw new LogicalOperatorException("given node is not WHERE! " + astNode.dump());
        if (astNode.getChildCount() != 1)
            throw new LogicalOperatorException("where clause has {} child, return" + astNode.getChildCount());
        FilterOperator whereOp = new FilterOperator(SQLConstant.TOK_WHERE);
        ASTNode child = astNode.getChild(0);
        analyzeWhere(child, child.getType(), whereOp);
        ((SFWOperator) initializedOperator).setFilterOperator(whereOp.getChildren().get(0));
    }

    private void analyzeWhere(ASTNode ast, int tokenIntType, FilterOperator filterOp)
            throws LogicalOperatorException {
        int childCount = ast.getChildCount();
        switch (tokenIntType) {
            case TSParser.KW_NOT:
                if (childCount != 1) {
                    throw new LogicalOperatorException("parsing where clause failed: children count != 1");
                }
                FilterOperator notOp = new FilterOperator(SQLConstant.KW_NOT);
                filterOp.addChildOperator(notOp);
                ASTNode childAstNode = ast.getChild(0);
                int childNodeTokenType = childAstNode.getToken().getType();
                analyzeWhere(childAstNode, childNodeTokenType, notOp);
                break;
            case TSParser.KW_AND:
            case TSParser.KW_OR:
                if (childCount != 2) {
                    throw new LogicalOperatorException("parsing where clause failed! node has " + childCount + " paramter.");
                }
                FilterOperator binaryOp = new FilterOperator(TSParserConstant.getTSTokenIntType(tokenIntType));
                filterOp.addChildOperator(binaryOp);
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
                BasicFunctionOperator basic = new BasicFunctionOperator(TSParserConstant.getTSTokenIntType(tokenIntType),
                        pair.left, pair.right);
                filterOp.addChildOperator(basic);
                break;
            default:
                throw new LogicalOperatorException("unsupported token:" + tokenIntType);
        }
    }

    private Pair<Path, String> parseLeafNode(ASTNode node) throws LogicalOperatorException {
        if (node.getChildCount() != 2)
            throw new LogicalOperatorException("leaf node children count != 2");
        ASTNode col = node.getChild(0);
        if (col.getType() != TSParser.TOK_PATH)
            throw new LogicalOperatorException("left node of leaf isn't PATH");
        Path seriesPath = parsePath(col);
        ASTNode rightKey = node.getChild(1);
        String seriesValue;
        if (rightKey.getType() == TSParser.TOK_PATH)
            seriesValue = parsePath(rightKey).getFullPath();
        else if (rightKey.getType() == TSParser.TOK_DATETIME) {
            seriesValue = parseTokenTime(rightKey);
        } else
            seriesValue = rightKey.getText();
        return new Pair<>(seriesPath, seriesValue);
    }

    private String parseTokenTime(ASTNode astNode) throws LogicalOperatorException {
        SimpleDateFormat sdf;
        sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");

        StringContainer sc = new StringContainer();
        for (int i = 0; i < astNode.getChildCount(); i++) {
            sc.addTail(astNode.getChild(i).getText());
        }
        Date date;
        try {
            date = sdf.parse(sc.toString());
        } catch (ParseException e) {
            throw new LogicalOperatorException("parse time error,String:" + sc.toString() + "message:" + e.getMessage());
        }
        return String.valueOf(date.getTime());
    }

    private Path parsePath(ASTNode node) {
        int childCount = node.getChildCount();
        String[] path = new String[node.getChildCount()];
        for (int i = 0; i < childCount; i++) {
            path[i] = node.getChild(i).getText().toLowerCase();
        }
        return new Path(new StringContainer(path, SystemConstant.PATH_SEPARATOR));
    }

    private Path parseRootPath(ASTNode node) {
        StringContainer sc = new StringContainer(SystemConstant.PATH_SEPARATOR);
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

    private void analyzeDataLoad(ASTNode astNode) throws IllegalASTFormatException {
        int childCount = astNode.getChildCount();
        // node path should have more than one level and first node level must be root
        if (childCount < 3 || !SQLConstant.ROOT.equals(astNode.getChild(1).getText().toLowerCase()))
            throw new IllegalASTFormatException("data load command: child count < 3\n" + astNode.dump());
        String csvPath = astNode.getChild(0).getText();
        if (csvPath.length() < 3 || csvPath.charAt(0) != '\'' || csvPath.charAt(csvPath.length() - 1) != '\'')
            throw new IllegalASTFormatException("data load: error format csvPath:" + csvPath);
        StringContainer sc = new StringContainer(SystemConstant.PATH_SEPARATOR);
        sc.addTail(SQLConstant.ROOT);
        for (int i = 2; i < childCount; i++) {
            String pathNode = astNode.getChild(i).getText().toLowerCase();
            sc.addTail(pathNode);
        }
        initializedOperator = new LoadDataOperator(SQLConstant.TOK_DATALOAD, csvPath.substring(1, csvPath.length() - 1),
                sc.toString());
    }

    private void analyzeAuthorCreate(ASTNode astNode) throws IllegalASTFormatException {
        int childCount = astNode.getChildCount();
        AuthorOperator authorOperator;
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
        initializedOperator = authorOperator;
    }

    private void analyzeAuthorUpdate(ASTNode astNode) throws IllegalASTFormatException {
        int childCount = astNode.getChildCount();
        AuthorOperator authorOperator;
        if (childCount == 1) {
            authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_UPDATE_USER, AuthorType.UPDATE_USER);
            ASTNode user = astNode.getChild(0);
            if (user.getChildCount() != 3) {
                throw new IllegalASTFormatException("illegal ast tree in create author command:\n" + astNode.dump());
            }
            System.out.println(user.dump());
            authorOperator.setUserName(parseStringWithQuoto(user.getChild(0).getText()));
            authorOperator.setNewPassword(parseStringWithQuoto(user.getChild(1).getText()));
        } else {
            throw new IllegalASTFormatException("illegal ast tree in create author command:\n" + astNode.dump());
        }
        initializedOperator = authorOperator;
    }

    private void analyzeAuthorDrop(ASTNode astNode) throws IllegalASTFormatException {
        int childCount = astNode.getChildCount();
        AuthorOperator authorOperator;
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
        } else {
            throw new IllegalASTFormatException("illegal ast tree in drop author command:\n" + astNode.dump());
        }
        initializedOperator = authorOperator;
    }

    private void analyzeAuthorGrant(ASTNode astNode) throws IllegalASTFormatException {
        int childCount = astNode.getChildCount();
        AuthorOperator authorOperator;
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
        initializedOperator = authorOperator;
    }

    private void analyzeAuthorRevoke(ASTNode astNode) throws IllegalASTFormatException {
        int childCount = astNode.getChildCount();
        AuthorOperator authorOperator;
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
        initializedOperator = authorOperator;
    }

}
