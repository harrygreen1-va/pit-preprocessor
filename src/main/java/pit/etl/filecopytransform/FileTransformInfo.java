package pit.etl.filecopytransform;

public class FileTransformInfo {
    private int numberOfColumns;
    private String stringToAppend;
    private String stringToAppendToHeader;
    
    public FileTransformInfo(int numberOfColumns) {
        super();
        this.numberOfColumns=numberOfColumns;
    }

    public FileTransformInfo(String stringToAppend, String stringToAppendToHeader) {
        super();
        this.stringToAppend=stringToAppend;
        this.stringToAppendToHeader=stringToAppendToHeader;
    }

    public int getNumberOfColumns() {
        return numberOfColumns;
    }

    public String getStringToAppend() {
        return stringToAppend;
    }

    public String getStringToAppendToHeader() {
        return stringToAppendToHeader;
    }
    
}
