package cn.edu.thu.tsfiledb.auth.dao;

public class DBDaoInitException extends Exception{
	private static final long serialVersionUID = -6713356921416511616L;
	
    public DBDaoInitException(String message) {
        super(message);
    }

    public DBDaoInitException(String message, Throwable cause) {
        super(message, cause);
    }

    public DBDaoInitException(Throwable cause) {
        super(cause);
    }

}
