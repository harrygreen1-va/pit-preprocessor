package pit.etl.filecopytransform.fbcs;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class FileInfoTest {

    @Test
    public void testFileInfoParsing() {
        DelimitedFileInfo fileInfo = DelimitedFileInfo.fromFile(new File("FBCS-ClaimsToScore-UB04-R4V4-20120604.txt"));
        System.err.println(fileInfo);

        assertTrue( fileInfo.isFileNameValid() );
        assertEquals( "R4V4", fileInfo.databaseId() );
        assertEquals( "FBCS", fileInfo.sourceSystem() );
        assertEquals( "UB04", fileInfo.formType() );
        assertEquals( "UB92", fileInfo.normalizedFormType() );

        assertEquals( "20120604", fileInfo.feedDateStr() );
        assertEquals( "FUC", fileInfo.processingCode() );
        assertEquals( "FBCS-ClaimsToScore-UB04-R4V4-C-20120604.txt", fileInfo.companionFile().getName() );
    }
    
    @SuppressWarnings("OptionalGetWithoutIsPresent") @Test
    public void testBadFileParsing() {
        DelimitedFileInfo fileInfo = DelimitedFileInfo.fromFile(new File("FBCS-ClaimsToScore-HCFA-R4V420120604.txt"));
        System.out.println( fileInfo );
        System.out.println( fileInfo.fileNameValidationError().get() );
        assertFalse( fileInfo.isFileNameValid() );
        // invalid date
        fileInfo = DelimitedFileInfo.fromFile(new File("FBCS-ClaimsToScore-HCFA-R4V4-2012.060423.txt"));
        System.out.println( fileInfo );
        System.out.println( fileInfo.fileNameValidationError().get() );
        assertFalse( fileInfo.isFileNameValid() );
    }

}