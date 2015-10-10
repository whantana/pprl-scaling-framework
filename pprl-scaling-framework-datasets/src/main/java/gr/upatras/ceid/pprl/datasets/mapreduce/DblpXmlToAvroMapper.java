package gr.upatras.ceid.pprl.datasets.mapreduce;

import gr.upatras.ceid.pprl.datasets.avro.dblp.DblpPublication;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.lang.StringEscapeUtils;
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
import java.util.regex.Pattern;

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
            dblpPublication.setKey(((Element) node).getAttribute("key"));
            dblpPublication.setAuthor(doc.getElementsByTagName("author").item(0).getTextContent());
            dblpPublication.setTitle(doc.getElementsByTagName("title").item(0).getTextContent());
            dblpPublication.setYear(doc.getElementsByTagName("year").item(0).getTextContent());
            context.write(new AvroKey<DblpPublication>(dblpPublication), NullWritable.get());
        } catch (SAXException e) {
            throw new IOException(e);
        }
    }
}
