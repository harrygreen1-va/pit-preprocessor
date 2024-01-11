package pit.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import pit.UnrecoverableException;


public class FilePathUtils {
    
    
    public static void touchOrTruncate(File file) {
        try{
            FileUtils.forceMkdirParent( file );
            if (file.exists()) {
                    new FileOutputStream(file).getChannel().truncate(0).close(); 
            } else {
                FileUtils.touch( file );
            }
        } catch (IOException e) {
            throw new UnrecoverableException( e );
        }
    }
    
    /**
     * Returns absolute path of a resource residing on the classpath
     * @param resource path to the resource on the classpath
     * @return absolute path in Unix format
     */
    
    public static String getAbsoluteFilePath( String resource ) {
        URL fileURL = FilePathUtils.class.getClassLoader().getResource(resource);
        if (fileURL==null)
            throw new RuntimeException("File "+resource+" not found");
        
        String absolutePath=fileURL.getPath();
        if (absolutePath.startsWith("file:"))
            absolutePath=absolutePath.substring("file:".length());

        if (absolutePath.endsWith("/"))
            absolutePath=absolutePath.substring(0, absolutePath.length()-1);
        
        return absolutePath;
    }

    public static String getPathForResourceInPackage( String packageName, String resourceName ) {
        String packagePath=packageName.replace(".", "/");
        String resourcePath=packagePath+"/"+resourceName;
        return getAbsoluteFilePath(resourcePath);
    }

    public static String getPathForResourceInPackage( Class<?> clazz, String resourceName ) {
        return getPathForResourceInPackage(clazz.getPackage().getName(), resourceName);
    }
    
    public static String getPathForResourceInPackage( Package pckg, String resourceName ) {
        return getPathForResourceInPackage(pckg.getName(), resourceName);
    }
    
    public static String toUnixPath( String path ){
        String nPath=path;
        if (nPath.contains("\\")) {
            nPath=nPath.replace("\\", "/");
        }
        if (nPath.contains(";")) {
            nPath=nPath.replace(";", ":");
        }
        
        return nPath;
    }
    
    public static boolean exists( String path ) {
        return new File(path).exists();
    }
    
    public static String getFileNameWithoutExt( String file ) {
        String fileName = new File(file).getName();
        int extPos = fileName.lastIndexOf('.');
        if ( extPos >=0 ) fileName=fileName.substring(0,extPos);
        return fileName;
    }
    
    public static String getExtension( String file ) {
        String fileName = new File(file).getName();
        String ext = null;
        int extPos = fileName.lastIndexOf('.');
        if ( extPos >=0 ) ext=fileName.substring(extPos+1);
        return ext;
    }
    
    public static String getHost(String url){
        if(url == null || url.length() == 0)
            return "";

        int doubleslash = url.indexOf("//");
        if(doubleslash == -1)
            doubleslash = 0;
        else
            doubleslash += 2;

        int end = url.indexOf('/', doubleslash);
        end = end >= 0 ? end : url.length();

        int port = url.indexOf(':', doubleslash);
        end = (port > 0 && port < end) ? port : end;

        return url.substring(doubleslash, end);
    }    
    
    public static String getRoot( String path ) {
        // if we have a protocol as part of the path, ignore it
        String prot=null;
        int protIdx=path.indexOf("://");
        if (protIdx>=0) {
            prot=path.substring(0,protIdx);
            path=path.substring(protIdx+1);
        }
        int startRootPos = 0;
        // We ignore all leading slashes
        while (path.charAt( startRootPos)=='/' )
            ++startRootPos;

        int endRootPos = path.indexOf('/', startRootPos );
        if (endRootPos < 0 )
            endRootPos = path.length();
        String newPath="";
        if (prot!=null)
            newPath=prot+"://";
        
        newPath+=path.substring( startRootPos, endRootPos );
        
        return newPath;
    }
    
    public static String toCanonicalPath(String path) {
        String canonicalPath;
        try {
            canonicalPath=new File(path).getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException("Error obtaining canonical path for "+path);
        }
        return canonicalPath;
    }
    
    
    public static String appendTimestamp( String file ){
        String fileNoExt=file;
        String ext="";
        int extPos = file.lastIndexOf('.');
        if ( extPos >=0 ) { 
            ext=file.substring(extPos);
            fileNoExt=file.substring(0, extPos);
        }
        String dateFormat="{0}_{1,date,yyyyMMdd_HHmmss}{2}";

        return MessageFormat.format( dateFormat, fileNoExt, new Date(),ext );
    }
    
    
    public static String getFileNameFromURL( String urlStr ){
    	// get rid of the protocol
        String path=null;
        int protIdx=urlStr.indexOf("://");
        if (protIdx>=0) {
            path=urlStr.substring(protIdx+1);
        }
        return new File(path).getName();
    }
    

    public static String removePrefix( String path ){
        path=normalize(path);
        String pathWOPrefix=StringUtils.substringAfter(path,":");
        
        if (!pathWOPrefix.startsWith("/"))
            pathWOPrefix="/"+pathWOPrefix;
        
        return pathWOPrefix;
    }
    
    public static String normalize(String path) {
        return FilenameUtils.normalize( path, true );
    }
    
}
