package gr.upatras.ceid.pprl.encoding;

import gr.upatras.ceid.pprl.datasets.QGramUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class FieldBloomFilterEncoding extends BloomFilterEncoding {

    protected Map<Schema.Field,BloomFilter> field2BloomFilterMap = new HashMap<Schema.Field, BloomFilter>();

    public FieldBloomFilterEncoding(){}

    public String getName() {
        return "FBF_" + super.getName();
    }

    public String toString() { return "FBF"; }

    public GenericData.Fixed encode(final Object obj, final Schema.Type type,
                                    final Schema.Field encodingField,
                                    final int N , final int K , final int Q)
            throws BloomFilterEncodingException {
        try{
            final String[] qGrams = QGramUtil.generateQGrams(obj,type,Q);

            if(!field2BloomFilterMap.containsKey(encodingField))
                field2BloomFilterMap.put(encodingField,new BloomFilter(N,K));
            else
                field2BloomFilterMap.get(encodingField).clear();

            for(String qGram : qGrams) field2BloomFilterMap.get(encodingField).addData(qGram.getBytes("UTF-8"));
            return new GenericData.Fixed(encodingField.schema(),field2BloomFilterMap.get(encodingField).getByteArray());
        } catch (NoSuchAlgorithmException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (InvalidKeyException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        } catch (UnsupportedEncodingException e) {
            throw new BloomFilterEncodingException(e.getMessage());
        }
    }
}
