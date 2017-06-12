package cn.edu.thu.tsfiledb.qp.exception;

/**
 * This exception is for calling error method in {@linkplain com.corp.tsfile.sql.exec.TSqlParserV2
 * QueryProcessor}.<br>
 * Throw this exception if user call {@code nonQuery} method from a {@code QUERY operator}, or call
 * {@code getIndex getIndex} method from a {@code NonQUERY operator}, like AUTHOR, LOADDATA,
 * UPDATE,INSERT, DELETE
 * 
 * @author kangrong
 *
 */
public class ErrorQueryOpException extends QueryProcessorException {

    private static final long serialVersionUID = -262405196554416563L;

    public ErrorQueryOpException(String msg) {
        super(msg);
    }

}
