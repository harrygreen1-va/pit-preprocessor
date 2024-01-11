package pit.etl.filecopytransform;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileProcessingException extends RuntimeException{
    private static final long serialVersionUID=-1392114530711253797L;
    
    private List<LineException> lineExceptions=new ArrayList<LineException>();
    private File file;

    public FileProcessingException(File file, String message) {
        super(message);
        this.file=file;
    }

    public File getFile() {
        return file;
    }
    
    public void addListException(LineException lineException){
        lineExceptions.add( lineException );    
    }
    
    public List<LineException> getLineExceptions(){
        return lineExceptions;
    }

    @Override
    public String getMessage(){
        return super.getMessage()+"\n"+createLinesMessage(lineExceptions);
    }
    
    private String createLinesMessage(List<LineException> lineExceptions) {
        StringBuilder buf=new StringBuilder();
        for(LineException lineException:lineExceptions) {
            buf.append( String.format( "%d : %s\n",lineException.getLineNumber(),lineException.getMessage() ));
        }
        return buf.toString();
    }
}
