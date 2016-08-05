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

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * DBLP to Avro Mapper class. Heavy lifting done in DbmlXmlInputFormat.
 */
public class DblpToAvroMapper extends Mapper<Text, TextArrayWritable, AvroKey<DblpPublication>, NullWritable> {

    @Override
    protected void map(Text key, TextArrayWritable value, Context context) throws IOException, InterruptedException {
        final DblpPublication dblpPublication = new DblpPublication();
        dblpPublication.setKey(key.toString());
        dblpPublication.setAuthor(value.getStrings()[0]); // author
        dblpPublication.setTitle(value.getStrings()[1]);  // title
        dblpPublication.setYear(value.getStrings()[2]);  // year
        context.write(new AvroKey<>(dblpPublication), NullWritable.get());
    }
}
