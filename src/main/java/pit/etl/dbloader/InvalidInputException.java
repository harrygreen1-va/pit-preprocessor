package pit.etl.dbloader;

/**
 * Exception used for wrapping system exceptions that are not meant to be recovered/handled, e.g., IOException 
 *
 */
public class InvalidInputException extends RuntimeException {

    
    public InvalidInputException(String msg, Object ... args) {
        super(makeMessage( msg, args));
    }
    
    public InvalidInputException(Throwable e) {
        super(e);
    }
    
    public InvalidInputException(String msg, Throwable e, Object ... args) {
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
