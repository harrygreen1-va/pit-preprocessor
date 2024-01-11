package pit.etl.filecopytransform;


public interface LineProcessor {
    String processLine(String line, int lineNumber);
}
