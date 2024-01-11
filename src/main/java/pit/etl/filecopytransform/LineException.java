package pit.etl.filecopytransform;

import java.io.File;

public class LineException extends Exception{
    private static final long serialVersionUID=-75560458053240672L;
    
    private int lineNumber=-1;
    private File file;
    
    public LineException(String message, int lineNumber) {
        super(message);
        this.lineNumber=lineNumber;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public File getFile() {
        return file;
    }

    public void setFile( File file ) {
        this.file=file;
    }
    

}
