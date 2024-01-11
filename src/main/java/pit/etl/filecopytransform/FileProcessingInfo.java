package pit.etl.filecopytransform;

import java.io.File;

public class FileProcessingInfo {
    private File sourceDir;
    private File destDir;
    
    private String includePattern;
    private String excludePattern;
    
    private FileProcessor processor;
    
    public FileProcessingInfo inlcude(String includePattern) {
        this.includePattern=includePattern;
        return this;
    }
    
    public FileProcessingInfo exclude(String excludePattern) {
        this.excludePattern=excludePattern;
        return this;
    }

    public FileProcessingInfo processWith(FileProcessor processor) {
        this.processor=processor;
        return this;
    }
    
    public FileProcessingInfo from(File sourceDir) {
        this.sourceDir=sourceDir;
        return this;
    }
   
    public FileProcessingInfo to(File destDir) {
        this.destDir=destDir;
        return this;
    }

    public File getSourceDir() {
        return sourceDir;
    }

    public File getDestDir() {
        return destDir;
    }

    public String getIncludePattern() {
        return includePattern;
    }

    public String getExcludePattern() {
        return excludePattern;
    }

    public FileProcessor getProcessor() {
        return processor;
    }    

    
    
}
