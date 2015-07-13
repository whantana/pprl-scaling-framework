package gr.upatras.ceid.pprl.datasets.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class HdfsPprlService {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsPprlService.class);

    private static final String USER_PATH = "/user/";

    private static final FsPermission PPRL_USER_PERMISSIONS = new FsPermission((short)740);

    private Configuration configuration;

    private Boolean withPermissions;

    private Map<String,Path> setup;

    public HdfsPprlService(final String linkingParty,
                           final List<String> dataParties, final Boolean withPermissions) {
        this. withPermissions = withPermissions;
        setup = new HashMap<String,Path>();
        setup.put(linkingParty,null);
        for (String party : dataParties) setup.put(party, null);
    }

    public void setConfiguration(final Configuration configuration) {
        this.configuration = configuration;
    }

    public void tearDown() throws IOException {
        for(String party : setup.keySet()) {
            tearDown(party);
        }
    }

    public void tearDown(final String party) throws IOException {
        final FileSystem fs = DistributedFileSystem.get(configuration);
        Path p = new Path(USER_PATH + party);
        if(fs.exists(p)) {
            fs.delete(p, true);
            setup.put(party,null);
            LOG.info("Deleting directory : " + USER_PATH + party);
        }
        fs.close();
    }

    public void setUp() throws IOException, InterruptedException {
        final FileSystem fs = DistributedFileSystem.get(configuration);
        final Path usersPath = new Path(USER_PATH);
        if(!fs.exists(usersPath)) {
            fs.mkdirs(usersPath);
            LOG.info("Creating directory : " + USER_PATH);
        }
        fs.close();
        for(String party : setup.keySet()) {
            setUp(party);
        }
    }

    public void setUp1(final String party) throws IOException {
        final FileSystem fs = DistributedFileSystem.get(configuration);
        final Path path = new Path(USER_PATH +  party );
        fs.mkdirs(path);
        if(withPermissions) {
            fs.setOwner(path,party,party);
            fs.setPermission(path, PPRL_USER_PERMISSIONS);
        }
        setup.put(party,path);
        LOG.info("Creating directory {} : " + USER_PATH + party,withPermissions?"with user permissions":"");
        fs.close();
    }

    public void setUp(final String party) throws IOException, InterruptedException {
        final Path path = new Path(USER_PATH +  party );
        if(!withPermissions) {
            final FileSystem fs = DistributedFileSystem.get(configuration);
            fs.mkdirs(path);
            fs.close();
        } else {
            UserGroupInformation ugi = UserGroupInformation.createProxyUser(party, UserGroupInformation.getLoginUser());
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    final FileSystem fs = DistributedFileSystem.get(configuration);
                    fs.mkdirs(path);
                    fs.setPermission(path, PPRL_USER_PERMISSIONS);
                    fs.close();
                    return null;
                }
            });
        }
        setup.put(party,path);
        LOG.info("Creating directory {} : " + USER_PATH + party,withPermissions?"with user permissions":"");
    }

    public Map<String,Path> getPprlSetup() {
        return setup;
    }

    public void checkForPprlSetup() throws IOException {
        if(setup == null ) setup = new HashMap<String,Path>();
        final FileSystem fs;
        fs = DistributedFileSystem.get(configuration);
        for ( String party : setup.keySet()) {
            Path path  = new Path(USER_PATH +  party );
            if(fs.exists(path)) setup.put(party,path);
        }
        fs.close();
    }

    @Override
    public String toString() {
        return "HdfsPprlService{" +
                "configuration=" + configuration +
                ", withPermissions=" + withPermissions +
                ", setup=" + setup +
                '}';
    }
}