package gr.upatras.ceid.pprl.datasets.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class HdfsPprlService {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsPprlService.class);

    public static final FsPermission PPRL_USER_PERMISSIONS = new FsPermission((short)740);
    public static final FsPermission PPRL_NO_PERMISSSIONS = new FsPermission((short)777);

    private Path pprlBasePath;
    private Path pprlFormatPath;

    private Configuration configuration;
    private FileSystem hdfs;

    private Boolean withPermissions;
    private Map<String,Path> format;
    private String linkingParty;

    public HdfsPprlService(final String linkingParty, final List<String> dataParties,
                           final Boolean withPermissions, final Configuration configuration) throws IOException {
        this.linkingParty = linkingParty;
        this.withPermissions = withPermissions;
        this.configuration = configuration;
        hdfs = DistributedFileSystem.get(configuration);
        pprlBasePath = new Path(hdfs.getUri() + "/user");
        pprlFormatPath = new Path(hdfs.getUri() + "/.pprlformat");
        format = new HashMap<String,Path>();
        format.put(linkingParty, null);
        for (String party : dataParties) format.put(party, null);
        LOG.info("HDFS PPRL Service initialized.");
        LOG.info(toString());
    }

    public void undoFormat() throws IOException {
        for(String party : format.keySet()) undoFormat(party);
    }

    public void undoFormat(final String party) throws IOException {
        if(hdfs.exists(getPprlPartyPath(party))) {
            hdfs.delete(getPprlPartyPath(party), true);
            format.put(party, null);
            LOG.info("Deleting directory : {}.", getPprlPartyPath(party));
        }
    }

    public void doFormat() throws IOException, InterruptedException {
        if(!hdfs.exists(pprlBasePath)) {
            hdfs.mkdirs(pprlBasePath);
            LOG.info("Creating base directory : {}.", pprlBasePath);
        }
        for(String party : format.keySet()) {
            doFormat(party);
        }
    }

    public void doFormat(final String party) throws IOException, InterruptedException {
        hdfs.mkdirs(getPprlPartyPath(party));
        if (withPermissions) {
            hdfs.setOwner(getPprlPartyPath(party),party,party);
            hdfs.setPermission(getPprlPartyPath(party), PPRL_USER_PERMISSIONS);
        } else hdfs.setPermission(getPprlPartyPath(party), PPRL_NO_PERMISSSIONS);
        format.put(party, getPprlPartyPath(party));
        LOG.info("Creating directory [user restricted ? {}] : {}.",
                withPermissions? "YES":"NO", getPprlPartyPath(party));
    }

    public Map<String,Path> getFormat() {
        return format;
    }

    public boolean validateFormat() throws IOException {
        if(format == null ) return false;
        for (Map.Entry<String,Path> entry : format.entrySet()) {
            if(!validateFormat(entry.getKey(), entry.getValue())) return false;
        }
        return true;
    }

    public Map<String,Boolean> getAvailableFormat() throws IOException {
        Map<String,Boolean> map = new HashMap<String,Boolean>();
        for (Map.Entry<String,Path> entry : format.entrySet()) {
            map.put(entry.getKey(),validateFormat(entry.getKey(), entry.getValue()));
        }
        return map;
    }

    public boolean validateFormat(final String party, final Path path) throws IOException {
        return path != null && path.equals(getPprlPartyPath(party)) && hdfs.exists(getPprlPartyPath(party));
    }

    public void saveFormatOnHdfs() throws IOException, InterruptedException {
        FSDataOutputStream fos = hdfs.create(pprlFormatPath,true);
        fos.writeChars(linkingParty + "\t" + format.get(linkingParty).toUri().toString() + "\n");
        for(Map.Entry<String,Path> entry : format.entrySet()) {
            if(entry.getKey().equals(linkingParty)) continue;
            fos.writeChars(entry.getKey() + "\t" + entry.getValue().toUri().toString() + "\n");
        }
        fos.close();
        if(withPermissions) {
            hdfs.setOwner(pprlFormatPath, linkingParty, linkingParty);
            hdfs.setPermission(pprlFormatPath, PPRL_USER_PERMISSIONS);
        } else hdfs.setPermission(pprlFormatPath, PPRL_NO_PERMISSSIONS);

        LOG.info("Saving format on HDFS : {}.",pprlFormatPath);
    }

    public void loadFormatFromHdfs() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(pprlFormatPath)));
        final String firstLine = br.readLine();
        if(firstLine == null) throw new IOException("First line is null/empty.");
        linkingParty = firstLine.split("\t")[0];
        format.put(linkingParty, new Path(firstLine.split("\t")[1]));
        for(String line;(line = br.readLine()) != null;) {
            String party = line.split("\t")[0];
            format.put(party, new Path(line.split("\t")[1]));
        }
        br.close();
        LOG.info("Loading format from HDFS : {}.",pprlFormatPath);
    }

    public void deleteFormatFromHdfs() throws IOException {
        hdfs.delete(pprlFormatPath,true);
        LOG.info("Deleting format from HDFS : {}.",pprlFormatPath);
    }

    @Override
    public String toString() {
        return "HdfsPprlService{" +
                "linkingParty=" + linkingParty +
                ", configuration=" + configuration +
                ", withPermissions=" + withPermissions +
                ", format=" + format +
                ", pprlBasePath=" + pprlBasePath +
                ", pprlFormatPath=" + pprlFormatPath +
                '}';
    }

    public void closeHdfs() throws IOException {
        if(hdfs != null ) hdfs.close();
    }

    public void openHdfs() throws IOException {
        if(hdfs != null) return;
        hdfs = DistributedFileSystem.get(configuration);
    }

    public void reopenHdfs() throws IOException {
        closeHdfs();
        openHdfs();
    }

    public Path getPprlPartyPath(final String party) {
        return new Path(pprlBasePath.toString() + "/" + party);
    }

    public Path getPprlFormatPath() {
        return pprlFormatPath;
    }

    public String getLinkingParty() {
        return linkingParty;
    }

    public Boolean isUserRestricted() {
        return withPermissions;
    }
}