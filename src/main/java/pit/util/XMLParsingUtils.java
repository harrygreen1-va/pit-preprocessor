package pit.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.FileUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.DOMBuilder;
import org.jdom2.input.StAXStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;



public class XMLParsingUtils {

    private final static Logger logger=LoggerFactory.getLogger( XMLParsingUtils.class );

    public static Document parseDOM( DocumentFragment domFragment ) {
        Document doc=new Document();
        DOMBuilder builder=new DOMBuilder();
        NodeList nodes=domFragment.getChildNodes();

        for (int i=0; i < nodes.getLength(); ++i) {
            Node node=nodes.item( i );
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                org.w3c.dom.Element domElement=(org.w3c.dom.Element) node;
                doc.addContent( (Element) builder.build( domElement ).clone() );
            }
        }
        return doc;
    }

    public static Document parseFile( File file ) {
        long startTime=System.nanoTime();
        Document doc=null;
        try {
            DocumentBuilder documentBuilder=getDomDocumentBuilder();
            org.w3c.dom.Document domDoc=documentBuilder.parse( file );
            doc=buildFromDomDoc(domDoc);
        }
        catch (Exception e) {
            raiseParsingException(e, null);
        }
        
        long elapsedTime=System.nanoTime() - startTime;
        logger.debug( "Parsed file in " + elapsedTime / 1000 / 1000 + " ms" );
        
        return doc;

    }

    private static DocumentBuilder getDomDocumentBuilder() throws ParserConfigurationException{
        DocumentBuilderFactory domFactory=DocumentBuilderFactory.newInstance();
        domFactory.setNamespaceAware( true );
        DocumentBuilder documentBuilder=domFactory.newDocumentBuilder();
        
        return documentBuilder;
    }
    
    private static Document buildFromDomDoc(org.w3c.dom.Document domDoc){
        DOMBuilder builder=new DOMBuilder();
        Document doc=builder.build( domDoc );
        return doc;
    }

    public static Document parse( InputStream input ) {
        long startTime=System.nanoTime();
        
        Document doc=null;
        try {
            
            DocumentBuilder documentBuilder=getDomDocumentBuilder();
            org.w3c.dom.Document domDoc=documentBuilder.parse( input );
            doc=buildFromDomDoc(domDoc);
        }
        catch (Exception e) {
            raiseParsingException(e, null);
        }

        long elapsedTime=System.nanoTime() - startTime;
        logger.debug( "Parsed input stream in " + elapsedTime / 1000 / 1000 + " ms" );

        return doc;
    }

    public static Document parseUsingStax( InputStream input ) {
        long startTime=System.nanoTime();
        
        XMLInputFactory xmlif =  XMLInputFactory.newInstance();
        
        Document doc=null;
        try {
            XMLStreamReader xmlr = xmlif.createXMLStreamReader(input);
            StAXStreamBuilder builder=new StAXStreamBuilder();
            doc=builder.build( xmlr );
        }
        catch (Exception e) {
            raiseParsingException(e, null);
        }

        long elapsedTime=System.nanoTime() - startTime;
        logger.debug( "Parsed input stream in " + elapsedTime / 1000 / 1000 + " ms" );

        return doc;
    }
    
    public static Document parseUsingStax( XMLStreamReader input  ) {
        long startTime=System.nanoTime();
        

        Document doc=null;
        try {
            StAXStreamBuilder builder=new StAXStreamBuilder();
            doc=builder.build( input );
        }
        catch (Exception e) {
            raiseParsingException(e, null);
        }

        long elapsedTime=System.nanoTime() - startTime;
        logger.debug( "Parsed input stream in " + elapsedTime / 1000 / 1000 + " ms" );

        return doc;
    }

    
    private static void raiseParsingException( Exception e, File file ) {
        if (file != null) {
            if (e instanceof FileNotFoundException ) {
                throw new XMLParsingException( "XML file '%s' does not exist", e, file.getAbsolutePath() );
            }
            if (e instanceof IOException) {
                throw new XMLParsingException( "Errors trying to read from file '%s'", e,
                                file.getAbsoluteFile() );
            }
            else {
                throw new XMLParsingException(
                                "Errors trying to parse xml file '%s'. Is this a valid XML file?", e,
                                file.getAbsoluteFile() );
            }
        }
        else
            throw new XMLParsingException( "Errors trying to parse XML", e );

    }

    public static void persistXML( File file, Document doc ) {
        try {
            FileUtils.writeStringToFile( file, JDomUtils.docToString( doc ), Charset.defaultCharset() );
        }
        catch (IOException ioException) {
            new XMLParsingException( "Error saving the file '%s'", ioException, file.getAbsolutePath() );
        }
    }

}
