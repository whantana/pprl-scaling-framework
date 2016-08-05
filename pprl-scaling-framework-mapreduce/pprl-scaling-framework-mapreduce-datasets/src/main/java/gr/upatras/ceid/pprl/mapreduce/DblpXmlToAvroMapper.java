package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.avro.dblp.DblpPublication;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * DBLP XML to Avro Mapper class.
 */
public class DblpXmlToAvroMapper extends Mapper<LongWritable, Text, AvroKey<DblpPublication>, NullWritable> {

    private DocumentBuilder dBuilder;

    private static final String XML_DBLP_START =
            "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>" +
            "<!DOCTYPE dblp SYSTEM \"http://dblp.uni-trier.de/xml/dblp.dtd\"><dblp>";
    private static final String XML_DBLP_END = "</dblp>";


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setValidating(false);
            dBuilder = factory.newDocumentBuilder();

        } catch (ParserConfigurationException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String xmlString = XML_DBLP_START + value.toString() + XML_DBLP_END;
        try {
            final InputSource input = new InputSource(new ByteArrayInputStream(xmlString.getBytes()));
            Document doc = dBuilder.parse(input);
            doc.getDocumentElement().normalize();

            final DblpPublication dblpPublication = new DblpPublication();
            final Node node = doc.getDocumentElement().getFirstChild();

            final String strKey = ((Element) node).getAttribute("key");
            final Node authorNode = doc.getElementsByTagName("author").item(0);
            final Node titleNode = doc.getElementsByTagName("title").item(0);
            final Node yearNode = doc.getElementsByTagName("year").item(0);


            final String author = (authorNode == null) ? "NULL" :  authorNode.getTextContent();
            final String title = (titleNode == null) ? "NULL" :  titleNode.getTextContent();
            final String year = (yearNode == null) ? "NULL" : yearNode.getTextContent();

            dblpPublication.setKey(strKey);
            dblpPublication.setAuthor(author);
            dblpPublication.setTitle(title);
            dblpPublication.setYear(year);
            context.write(new AvroKey<>(dblpPublication), NullWritable.get());
        } catch (SAXException e) {
            throw new IOException(e);
        }
    }
}
