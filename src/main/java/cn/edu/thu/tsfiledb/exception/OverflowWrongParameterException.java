package cn.edu.thu.tsfiledb.exception;

/**
 * Used for IntervalTree pass wrong parameters. </br>
 * e.g. TSDataType inconsistent;
 * e.g. start time is greater than end time.
 *
 * @author CGF
 */
public class OverflowWrongParameterException extends DeltaEngineRunningException{
	private static final long serialVersionUID = 5386506095896639099L;

	public OverflowWrongParameterException(String message, Throwable cause) { super(message, cause);}

    public OverflowWrongParameterException(String message) {
        super(message);
    }

    public OverflowWrongParameterException(Throwable cause) {
        super(cause);
    }
}
