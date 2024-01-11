package pit.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.commons.lang3.StringUtils;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.filter.ElementFilter;
import org.jdom2.output.Format;
import org.jdom2.output.LineSeparator;
import org.jdom2.output.XMLOutputter;
import org.jdom2.util.IteratorIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pit.UnrecoverableException;



public class JDomUtils {

    @SuppressWarnings("unused")
    private final Logger logger=LoggerFactory.getLogger( getClass() );


    public static Element addOrUpdateElement( Element parentElt, String eltName, String text ) {
        Element childElt=parentElt.getChild( eltName );
        if (childElt == null) {
            childElt=new Element( eltName );
            parentElt.addContent( childElt );
        }
        childElt.setText( text );

        return childElt;
    }

    /**
     * Creates a copy of the element with the same name/namespace.
     * Not in use.
     * @param element
     * @return shallow copy of the element
     */
    public static Element cloneElementShallow(Element element){
        Element newElt=new Element(element.getName(), element.getNamespace());
        newElt.setContent( element.getContent() );
        newElt.setAttributes( element.getAttributes() );
        return newElt;
    }
    
    public static Element addOrUpdateElement( Element parentElt, String eltName, boolean bool ) {
        return JDomUtils.addOrUpdateElement( parentElt, eltName, Boolean.toString( bool ).toLowerCase() );
    }

    public static String docToString( Document doc ) {
        XMLOutputter outputter=new XMLOutputter( getDefaultXmlFormat() );
        StringBuilderWriter writer=new StringBuilderWriter();
        try {
            outputter.output( doc, writer );
        }
        catch (IOException ioe) {
            throw new RuntimeException( ioe );
        }

        return writer.getBuilder().toString();
    }
    
    
    public static String xmlFragmentToString( Element elt ) {
        XMLOutputter outputter=new XMLOutputter( getDefaultXmlFormat() );
        StringBuilderWriter writer=new StringBuilderWriter();
        try {
            outputter.output( elt, writer );
        }
        catch (IOException ioe) {
            throw new RuntimeException( ioe );
        }

        return writer.getBuilder().toString();
    }

    public static Format getDefaultXmlFormat() {
        Format format=Format.getPrettyFormat();
        format.setIndent( "    " );
        format.setLineSeparator( LineSeparator.SYSTEM );
        
        return format;
    }

    public static byte[] docToBytes( Document doc ) {

        XMLOutputter outputter=new XMLOutputter( Format.getCompactFormat() );

        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        try {
            outputter.output( doc, baos );
        }
        catch (IOException ioe) {
            throw new RuntimeException( ioe );
        }
        return baos.toByteArray();
    }

    
    public static void docToOutputStream( Document doc, OutputStream outputStream ) {

        XMLOutputter outputter=new XMLOutputter( Format.getCompactFormat() );

        try {
            outputter.output( doc, outputStream );
        }
        catch (IOException ioe) {
            throw new RuntimeException( ioe );
        }
    }
    
    
    public static String getRequiredAttributeValue( Element elt, String attrName ) {
        String val=elt.getAttributeValue( attrName );

        if (val == null)
            throw new UnrecoverableException( "Element '%s' is missing required attribute '%s'",
                            elt.getName(), attrName );
        return val.trim();
    }

    public static String getRequiredChildElementValue( Element elt, String eltName ) {
        String val=elt.getChildTextTrim( eltName );

        if (val == null)
            throw new UnrecoverableException( "Element '%s' is missing required child element '%s'",
                            elt.getName(), eltName );
        return val.trim();
    }

    
    public static Element getSingleChild( Element parentElt ) {
        List<Element> children=parentElt.getChildren();
        if (children.size()==0)
            throw new UnrecoverableException( "Element '%s' does not have any children", parentElt.getName() );
        if (children.size()>1)
            throw new UnrecoverableException( "Element '%s' has multiple children, only one was expected", parentElt.getName() );
        
        return children.get( 0 );
    }

    
    public static String elementsToString( List<Element> elts ) {
        StringBuilder buf=new StringBuilder();
        for (Element elt : elts) {
            if (buf.length() > 0)
                buf.append( ", " );
            buf.append( elementToString( elt ) );
        }
        return buf.toString();
    }

    public static String attributesToString( List<Attribute> attrs ) {
        StringBuilder buf=new StringBuilder();
        for (Attribute attr : attrs) {
            if (buf.length() > 0)
                buf.append( ", " );
            buf.append( attributeToString( attr ) );
        }
        return buf.toString();
    }

    public static String attributeToString( Attribute attr ) {
        StringBuilder buf=new StringBuilder();
        buf.append( attr.getParent().getName() + "/@" );
        buf.append( attr.getName() );
        return buf.toString();
    }

    public static String elementToString( Element elt ) {
        StringBuilder buf=new StringBuilder();
        if (elt.getParentElement() != null)
            buf.append( elt.getParentElement().getName() + "/" );
        buf.append( elt.getName() );
        return buf.toString();
    }

    /**
     * Formats element as "function string", i.e.: parent(child1:text,
     * child2:text)
     * 
     * @param parentElt
     * @return
     */
    public static String elementWithClidrenToFunctionString( Element parentElt ) {

        List<Element> children=parentElt.getChildren();
        // if the element has text, return name:text
        if (children.size() == 0)
            return elementToNameValueString( parentElt );

        StringBuilder buf=new StringBuilder();
        buf.append( parentElt.getName() + "( " );
        
        StringBuilder childrenBuf=new StringBuilder();
        for (Element childElt : children) {
            if ( StringUtils.isNotBlank( childrenBuf ) )
                childrenBuf.append(", ");
            childrenBuf.append( elementToNameValueString( childElt ) );
        }
        
        if (StringUtils.isNotBlank( childrenBuf ))
            buf.append( childrenBuf );
        
        buf.append( " )" );

        return buf.toString();

    }

    private static String elementToNameValueString( Element elt ) {
        StringBuilder buf=new StringBuilder();

        buf.append( elt.getName() );

        String text=elt.getText();
        if (text != null && text.trim().length() > 0)
            buf.append( "=\"" + text + '\"' );

        return buf.toString();
    }

    /**
     * Clears the namespace from the supplied element and all of its descendants
     * 
     * @param element
     */
    public static void clearNamespace( Element element, String namespaceStartsWith ) {

        IteratorIterable<Element> elts=element.getDescendants( new ElementFilter() );
        for (Element elt : elts) {
            clearElementNamespace( elt, namespaceStartsWith );
        }
        clearElementNamespace( element, namespaceStartsWith );
    }

    private static void clearElementNamespace( Element elt, String namespaceStartsWith ) {
        String nsURI=elt.getNamespaceURI();
        if (nsURI.startsWith( namespaceStartsWith )) {
            elt.setNamespace( null );
        }
    }

}
