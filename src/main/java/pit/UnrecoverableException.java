package pit;

/**
 * Exception used for wrapping system exceptions that are not meant to be recovered/handled, e.g., IOException 
 *
 */
public class UnrecoverableException extends RuntimeException {

    
    public UnrecoverableException(String msg, Object ... args) {
        super(makeMessage( msg, args));
    }
    
    public UnrecoverableException(Throwable e) {
        super(e);
    }
    
    public UnrecoverableException(String msg, Throwable e, Object ... args) {
        super(makeMessage( msg, args), e);
    }
    
    
    private static String makeMessage( String message, Object ...  args){
        if (args.length==0)
            return message;
        else
            return String.format(message, args);
    }
    
    
    private static final long serialVersionUID = 1L;
    
}
