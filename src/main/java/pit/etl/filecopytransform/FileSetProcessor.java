package pit.etl.filecopytransform;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSetProcessor {

    private final Logger logger=LoggerFactory.getLogger( getClass() );
    
    private Collection<FileProcessingInfo> fileProcessingInfos=new ArrayList<FileProcessingInfo>();

    public FileSetProcessor() {

    }

    public FileSetProcessor(FileProcessingInfo fileProcessingInfo) {
        addFileProcessingInfo( fileProcessingInfo );
    }

    public void addFileProcessingInfo( FileProcessingInfo fileProcessingInfo ) {
        fileProcessingInfos.add( fileProcessingInfo );
    }

    public void process() {
        for (FileProcessingInfo fileProcessingInfo : fileProcessingInfos) {

            List<File> filesToProcess=findInputFiles( fileProcessingInfo.getSourceDir(),
                            fileProcessingInfo.getIncludePattern(),
                            fileProcessingInfo.getExcludePattern() );

            for (File fileToProcess : filesToProcess) {
                processFile( fileProcessingInfo.getProcessor(), fileToProcess,
                                fileProcessingInfo.getDestDir() );
            }
        }
    }


    private void processFile( FileProcessor fileProcessor, File inputFile, File targetDir ) {
        File outputFile=new File( targetDir, inputFile.getName() );
        // TODO: process in ||
        logger.debug( "Processing file {} using the processor {}", inputFile.getName(), fileProcessor
                        .getClass().getName() );
        fileProcessor.processFile( inputFile, outputFile );
    }

    private List<File> findInputFiles( File inputDir, String includeFilePattern,
                    String excludeFilePattern ) {
        List<IOFileFilter> filters=new ArrayList<IOFileFilter>();
        if (includeFilePattern != null)
            filters.add( new WildcardFileFilter( includeFilePattern ) );
        if (excludeFilePattern != null)
            filters.add( new NotFileFilter( new WildcardFileFilter( excludeFilePattern ) ) );

        AndFileFilter mainAndFilter=new AndFileFilter();
        mainAndFilter.setFileFilters( filters );

        File[] files=inputDir.listFiles( (FileFilter) mainAndFilter );
        logger.info( "Found {} matching files matching the pattern '{}' with exclude pattern '{}'", files.length
                        , includeFilePattern, excludeFilePattern );
        return Arrays.asList( files );
    }

}
