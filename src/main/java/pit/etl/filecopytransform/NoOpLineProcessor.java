package pit.etl.filecopytransform;

public class NoOpLineProcessor implements LineProcessor{

    @Override
    public String processLine( String line, int lineNumber ) {
        return line;
    }

}
