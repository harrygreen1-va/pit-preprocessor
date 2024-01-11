package pit.util;

import pit.UnrecoverableException;



public class XMLParsingException extends UnrecoverableException {
    
    public XMLParsingException(Throwable e) {
        super(e);
    }
    
    public XMLParsingException(String msg, Throwable e, Object ... args) {
        super( msg, e, args);
    }
    
    
    private static final long serialVersionUID = 1L;    

}
