package gr.upatras.ceid.pprl.service.matching;

import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.mapreduce.ToolRunner;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

@Service
public class MatchingService implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalMatchingService.class);

    public void afterPropertiesSet() {
        LOG.info("Matching service initialized[Tool#1 = %s]",
                (exhaustiveRecordPairSimilarityToolRunner != null));
    }

    public static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);


    @Autowired
    private ToolRunner exhaustiveRecordPairSimilarityToolRunner;

    @Autowired
    private FileSystem hdfs;

    public SimilarityVectorFrequencies createMatrix(final Path inputPath, final Path inputSchemaPath,
                                         final String uidFieldName,
                                         final long recordCount, final int reducerCount,
                                         final Path basePath,
                                         final String matrixName,
                                         final String[] fieldNames)
            throws Exception {
        try {
            if (!hdfs.exists(basePath)) hdfs.mkdirs(basePath, ONLY_OWNER_PERMISSION);
            final Path statsPath = new Path(basePath, String.format("%s.properties",matrixName));
            runExhaustiveRecordPairSimilarityTool(inputPath, inputSchemaPath, statsPath, fieldNames,
                    uidFieldName, recordCount, reducerCount);
            final SimilarityVectorFrequencies matrix = new SimilarityVectorFrequencies();
            final Properties properties = new Properties();
            final FSDataInputStream fsdis = hdfs.open(statsPath);
            properties.load(fsdis);
            fsdis.close();
            matrix.fromProperties(properties);
            return matrix;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public void runExhaustiveRecordPairSimilarityTool(final Path inputPath, final Path inputSchemaPath,
                                                      final Path propertiesOutputPath,
                                                      final String[] fieldNames,
                                                      final String uidFieldName,
                                                      final long recordCount, final int reducerCount)
            throws Exception {
        if(exhaustiveRecordPairSimilarityToolRunner == null) throw new IllegalStateException("tool-runner not set");
        LOG.info("input={}", inputPath);
        LOG.info("inputSchemaPath={}", inputSchemaPath);
        LOG.info("propertiesOutputPath={}",propertiesOutputPath);
        LOG.info("uidFieldName={}",uidFieldName);
        LOG.info("recordCount={}",recordCount);
        LOG.info("reducerCount={}",reducerCount);
        final StringBuilder fsb = new StringBuilder(fieldNames[0]);
        if(fieldNames.length > 1)
            for (int i = 1; i <fieldNames.length; i++)
                fsb.append(",").append(fieldNames[i]);
        LOG.info("fieldNames={}", fsb.toString());
        exhaustiveRecordPairSimilarityToolRunner.setArguments(
                inputPath.toString(),inputSchemaPath.toString(),
                propertiesOutputPath.toString(),
                uidFieldName,String.valueOf(recordCount),
                fsb.toString(),String.valueOf(reducerCount)
        );
        exhaustiveRecordPairSimilarityToolRunner.call();
    }

    public ExpectationMaximization newEMInstance(final String[] fieldNames, final double[] m, final double[] u, double p) {
        LOG.info(String.format("New EM Instance [fieldNames=%s,m=%s,u=%s,p=%f].",
                Arrays.toString(fieldNames),Arrays.toString(m),Arrays.toString(u),p));
        return new ExpectationMaximization(fieldNames,m,u,p);
    }

    public ExpectationMaximization newEMInstance(final String[] fieldNames, final double m, final double u, double p) {
        LOG.info(String.format("New EM Instance [fieldNames=%s,m=%f,u=%f,p=%f].",
                Arrays.toString(fieldNames),m,u,p));
        return new ExpectationMaximization(fieldNames,m,u,p);
    }

    public ExpectationMaximization newEMInstance(final String[] fieldNames) {
        return newEMInstance(fieldNames,0.9,0.9,0.001);
    }
}
