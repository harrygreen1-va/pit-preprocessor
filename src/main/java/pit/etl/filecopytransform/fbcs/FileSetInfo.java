package pit.etl.filecopytransform.fbcs;

public class FileSetInfo {

    private String fileIncludePattern;

    public FileSetInfo(String fileIncludePattern) {
        super();
        this.fileIncludePattern=fileIncludePattern;
    }

    public String sourceSystem() {
        return FbcsFileNameParser.getSystem( fileIncludePattern )
                        .orElseThrow( () -> new IllegalArgumentException(
                                        "Can't infer system from the file mask "
                                            + fileIncludePattern ) );
    }
    
    public String formType() {
        return FbcsFileNameParser.getFormType( fileIncludePattern )
                        .orElseThrow( () -> new IllegalArgumentException(
                                        "Can't infer form type (UB/HCFA) from the file mask "
                                            + fileIncludePattern ) );
    }

    public String processingCode() {
        return FbcsFileNameParser.getProcessingCode( fileIncludePattern );
    }

}
