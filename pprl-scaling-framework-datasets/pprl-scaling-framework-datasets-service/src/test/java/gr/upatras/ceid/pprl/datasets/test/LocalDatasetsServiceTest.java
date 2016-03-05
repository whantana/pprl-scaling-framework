package gr.upatras.ceid.pprl.datasets.test;


import gr.upatras.ceid.pprl.base.CombinatoricsUtil;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetFieldStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.service.LocalDatasetsService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertNotNull;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:local-datasets-test-context.xml")
public class LocalDatasetsServiceTest {

    private static Logger LOG = LoggerFactory.getLogger(LocalDatasetsServiceTest.class);

    @Autowired
    private LocalDatasetsService lds;

    private Path[] AVRO_PATHS;
    private Path SCHEMA_PATH;
    private String[] FIELDS =  new String[]{"name", "surname"};

    @Before
    public void setUp() throws IOException {
        assertNotNull(lds);
        assertNotNull(lds.getLocalFS());
        if (lds.getLocalFS().getConf() == null) {
            lds.getLocalFS().initialize(URI.create("file:///"), new Configuration());
        }
        AVRO_PATHS = new Path[]{new Path("person_small/avro")};
        SCHEMA_PATH = new Path("person_small/schema/person_small.avsc");
    }

    @Test
    public void test0() throws IOException, DatasetException {
        final Schema schema = lds.loadSchema(SCHEMA_PATH);
        LOG.info(schema.toString(true));
    }

    @Test
    public void test1() throws IOException, DatasetException {
        final GenericRecord[] sample = lds.sample(AVRO_PATHS, SCHEMA_PATH, 20);
        final Schema schema = lds.loadSchema(SCHEMA_PATH);
        final String sampleName = "person_small_sample";
        lds.saveRecords(sampleName, sample, schema);
    }

    @Test
    public void test2() throws IOException, DatasetException {
        final GenericRecord[] records = lds.loadRecords(AVRO_PATHS, SCHEMA_PATH);
        final Schema schema = lds.loadSchema(SCHEMA_PATH);
        final DatasetStatistics statistics = new DatasetStatistics();
        statistics.setRecordCount(records.length);
        statistics.setEmPairs(CombinatoricsUtil.twoCombinationsCount(records.length));
        statistics.setFieldNames(FIELDS);
        DatasetStatistics.calculateQgramStatistics(records, schema, statistics, FIELDS);
        final double[] estimatedM = new double[]{0.9,0.9};
        final double[] estimatedU = new double[]{0.1,0.5};
        final double estimatedP = 0.01;
        final int iterations = 3;
        statistics.setEmAlgorithmIterations(iterations);
        statistics.setP(estimatedP);
        DatasetStatistics.calculateStatsUsingEstimates(
                statistics,FIELDS,
                estimatedM,estimatedU);
        StringBuilder report = new StringBuilder(prettyStats(statistics));
        report.append(prettyBFEStats(statistics.getFieldStatistics(), 15, 2));
        report.append(prettyBFEStats(statistics.getFieldStatistics(), 15, 3));
        report.append(prettyBFEStats(statistics.getFieldStatistics(), 15, 4));
        LOG.info(report.toString());
        final String reportName = "stats_report";
        lds.saveStats(reportName, statistics);
        lds.saveStats(reportName, statistics, new Path("person_small"));
        lds.saveStats(reportName, statistics, new Path("asdf"));
    }


    @Test
    public void test3() throws IOException, DatasetException {
        Path[] avroPaths = new Path[]{new Path("random/avro/random.avro")};
        Path schemaPath = new Path("random/schema/random.avsc");
        LOG.info(
                prettyRecords(
                        lds.loadRecords(avroPaths, schemaPath),
                        lds.loadSchema(schemaPath)
                )
        );

        schemaPath = new Path("da_int/schema/da_int.avsc");

        avroPaths = new Path[]{
                new Path("da_int/avro/da_int_1.avro"),
                new Path("da_int/avro/da_int_2.avro")
        };
        LOG.info(
                prettyRecords(
                        lds.loadRecords(avroPaths, schemaPath),
                        lds.loadSchema(schemaPath)
                )
        );

        avroPaths = new Path[]{
                new Path("da_int/avro/da_int_1.avro"),
                new Path("da_int/avro/da_int_2.avro"),
                new Path("da_int/avro/da_int_3.avro")
        };
        LOG.info(
                prettyRecords(
                        lds.loadRecords(avroPaths, schemaPath),
                        lds.loadSchema(schemaPath)
                )
        );

        avroPaths = new Path[]{
                new Path("da_int/avro")
        };
        LOG.info(
                prettyRecords(
                        lds.loadRecords(avroPaths, schemaPath),
                        lds.loadSchema(schemaPath)
                )
        );
    }


    private static String prettyRecords(final GenericRecord[] records,
                                        final Schema schema) {
        final StringBuilder sb = new StringBuilder();
        final List<Schema.Field> fields = schema.getFields();
        final List<Schema.Type> types = new ArrayList<Schema.Type>();
        final List<String> fieldNames = new ArrayList<String>();

        for (int i = 0; i < fields.size() ; i++) {
            fieldNames.add(i, fields.get(i).name());
            types.add(i,fields.get(i).schema().getType());
        }
        sb.append("#Records =").append(records.length).append("\n");
        sb.append("#Fields =").append(fields.size()).append("\n");

        final StringBuilder hsb = new StringBuilder();
        for (int i = 0; i < fields.size() ; i++) {
            final Schema.Type type = types.get(i);
            final String mod = (type.equals(Schema.Type.FIXED)) ?
                    String.format("%%%ds|",fields.get(i).schema().getFixedSize() * 8 + 5): "%25s|";
            hsb.append(String.format(mod, String.format("%s (%s)", fieldNames.get(i),types.get(i))));
        }
        final String header = hsb.toString();
        sb.append(header).append("\n");
        sb.append(new String(new char[header.length()]).replace("\0", "-")).append("\n");
        for (GenericRecord record : records) {
            final StringBuilder rsb = new StringBuilder();
            for (int i = 0; i < fields.size(); i++) {
                final String fieldName = fieldNames.get(i);
                final Schema.Type type = types.get(i);
                if (type.equals(Schema.Type.FIXED)) {
                    GenericData.Fixed fixed = (GenericData.Fixed) record.get(i);
                    String val = prettyBinary(fixed.bytes());
                    rsb.append(String.format(String.format("%%%ds|", fixed.bytes().length * 8 + 5), val));
                } else {
                    String val = String.valueOf(record.get(fieldName));
                    if (val.length() > 25) {
                        val = val.substring(0, 10) + "..." + val.substring(val.length() - 10);
                    }
                    rsb.append(String.format("%25s|", val));
                }
            }
            sb.append(rsb.toString()).append("\n");
        }

        return sb.toString();
    }

    private static String prettyBinary(final byte[] binary) {
        final StringBuilder sb = new StringBuilder();
        for (int i = (binary.length - 1); i >= 0 ; i--) {
            byte b = binary[i];
            sb.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
        }
        return sb.toString();
    }


    private static String prettyStats(DatasetStatistics statistics) {
        final StringBuilder sb = new StringBuilder();
        sb.append("#Records=").append(statistics.getRecordCount()).append("\n");
        sb.append("#Fields=").append(statistics.getFieldCount()).append("\n");
        sb.append("#Pairs=").append(statistics.getEmPairs()).append("\n");
        sb.append("#Expectation Maximization Estimator iterations=")
                .append(statistics.getEmAlgorithmIterations())
                .append("\n");
        sb.append("#Estimated Duplicate Portion(p)=")
                .append(String.format("%.3f", statistics.getP()))
                .append("\n");
        final StringBuilder hsb = new StringBuilder(String.format("%50s","Metric\\Field name"));
        final Set<String> fieldNames = statistics.getFieldStatistics().keySet();
        for (String fieldName : fieldNames)
            hsb.append(String.format("|%25s", fieldName));
        final String header = hsb.toString();
        sb.append(header).append("\n");
        sb.append(new String(new char[header.length()]).replace("\0", "-")).append("\n");
        for (int i = 0; i < DatasetFieldStatistics.description.length; i++) {
            final StringBuilder ssb = new StringBuilder(
                    String.format("%50s",DatasetFieldStatistics.description[i]));
            for (String fieldName : fieldNames)
                ssb.append(String.format("|%25s",
                        String.format("%.5f", statistics.getFieldStatistics().get(fieldName).getStats()[i])));
            sb.append(ssb.toString()).append("\n");
        }
        sb.append(new String(new char[header.length()]).replace("\0", "-")).append("\n");

        return sb.toString();
    }


    private static String prettyBFEStats(Map<String,DatasetFieldStatistics> fieldStatistics,final int K, final int Q) {
        assert K > 0 && Q >= 2 && Q <= 4;
        final StringBuilder sb = new StringBuilder();
        final String bar = new String(new char[50 + fieldStatistics.keySet().size()*25]).replace("\0", "-");
        Map<String,Integer> fbfNs = new HashMap<String,Integer>();
        Map<String,Integer> rbfNs = new HashMap<String,Integer>();

        sb.append("#Encoding Bloom Filters K=").append(K).append("\n");
        sb.append("#Encoding Bloom Filters Q=").append(Q).append("\n");
        sb.append(bar).append("\n");
        StringBuilder ssb = new StringBuilder(String.format("%50s","Dynamic FBF length"));
        for (String fieldName : fieldStatistics.keySet()) {
            double g = fieldStatistics.get(fieldName).getQgramCount(Q);
            int fbfN = (int) Math.ceil((1 / (1 - Math.pow(0.5, (double) 1 / (g * K)))));
            fbfNs.put(fieldName,fbfN);
            ssb.append(String.format("|%25s", String.format("%d",fbfN)));
        }
        sb.append(ssb.toString()).append("\n");

        ssb = new StringBuilder(String.format("%50s","Candidate RBF length"));
        for (String fieldName : fieldStatistics.keySet()) {
            double fbfN = fbfNs.get(fieldName);
            double nr = fieldStatistics.get(fieldName).getNormalizedRange();
            int rbfN  = (int) Math.ceil(fbfN/ nr);
            rbfNs.put(fieldName,rbfN);
            ssb.append(String.format("|%25s", String.format("%d", rbfN)));
        }
        sb.append(ssb.toString()).append("\n");

        int rbfN = Collections.max(rbfNs.values());
        sb.append(bar).append("\n");
        sb.append("#RBF length=").append(rbfN).append("\n");
        sb.append(bar).append("\n");

        ssb = new StringBuilder(String.format("%50s","Selected bit length"));
        for (String fieldName : fieldStatistics.keySet()) {
            double nr = fieldStatistics.get(fieldName).getNormalizedRange();
            int selectedBitCount = (int)Math.ceil((double) rbfN * nr);
            ssb.append(String.format("|%25s",
                    String.format("%d (%.1f %%)",
                            selectedBitCount,nr*100)
            ));
        }
        sb.append(ssb.toString()).append("\n");

        return sb.toString();
    }
}
