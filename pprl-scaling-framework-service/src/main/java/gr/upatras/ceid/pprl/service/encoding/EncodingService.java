package gr.upatras.ceid.pprl.service.encoding;

import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class EncodingService implements InitializingBean{

    private static final Logger LOG = LoggerFactory.getLogger(EncodingService.class);

    public void afterPropertiesSet() {
        LOG.info(String.format("Encoding service initialized [Tool#1 = %s]",
                (encodeDatasetToolRunner != null)));
    }

    @Autowired
    private ToolRunner encodeDatasetToolRunner;

    public void runEncodeDatasetTool(final Path input, final Path inputSchema,
                                     final Path output, final Path outputSchema)
            throws Exception {
        try {
            final List<String> argsList = new ArrayList<String>();
            argsList.add(input.toString()); argsList.add(inputSchema.toString());
            LOG.info("input={} , inputSchema={}", input, inputSchema);
            argsList.add(output.toString()); argsList.add(outputSchema.toString());
            LOG.info("output={} , outputSchema={}", output, outputSchema);
            String[] args = new String[argsList.size()];
            args = argsList.toArray(args);
            LOG.debug("args={}", Arrays.toString(args));
            encodeDatasetToolRunner.setArguments(args);
            encodeDatasetToolRunner.call();
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }
}
