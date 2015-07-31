package gr.upatras.ceid.pprl.datasets.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;


@Service
public class HdfsPprlService {

    public static final FsPermission PPRL_ONLY_USER_PERMISSIONS =
            new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);
    public static final FsPermission PPRL_USER_GROUP_PERMISSIONS =
            new FsPermission(FsAction.ALL, FsAction.READ, FsAction.NONE, false);
    public static final FsPermission PPRL_FRAMEWORK_DIRS_PERMS =
            new FsPermission(FsAction.ALL, FsAction.READ, FsAction.READ, false);
    public static final FsPermission PPRL_NO_PERMISSIONS =
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL, false);
    private static final Logger LOG = LoggerFactory.getLogger(HdfsPprlService.class);
    private Path pprlBasePath;

    @Autowired
    private Configuration hadoopConfiguration;
    private FileSystem hdfs;

    private Boolean withPermissions;
    private FsPermission partyPathPermissions;
    private FsPermission basePathPermissions;
    private FsPermission uploadedFilesPermissions;

    public HdfsPprlService(final Boolean userRestricted,
                           final String pprlBasePathStr) {
        withPermissions = userRestricted;
        partyPathPermissions = withPermissions ? PPRL_USER_GROUP_PERMISSIONS : PPRL_NO_PERMISSIONS;
        basePathPermissions = withPermissions ? PPRL_FRAMEWORK_DIRS_PERMS : PPRL_NO_PERMISSIONS;
        uploadedFilesPermissions = withPermissions ? PPRL_ONLY_USER_PERMISSIONS : PPRL_NO_PERMISSIONS;
        pprlBasePath = new Path(pprlBasePathStr);
        hdfs = null;
        LOG.info("HDFS PPRL Service initialized.");
    }

    public void assertHdfsOpen() throws IOException {
        if (hdfs == null) throw new IOException("HDFS connection is closed.");
    }

    public void assertBasePathExists() throws IOException {
        if (!hdfs.exists(pprlBasePath)) throw new IOException("Base path does not exist on hdfs : " + pprlBasePath);
    }

    public Path getPprlPartyPath(final String party) {
        return new Path(pprlBasePath + "/" + party);
    }

    public void openHdfs() throws IOException {
        hdfs = DistributedFileSystem.get(hadoopConfiguration);
        if (!pprlBasePath.toString().contains("hdfs://"))
            pprlBasePath = new Path(hdfs.getUri() + pprlBasePath.toString());
        LOG.info("HDFS connection is now opened.");
    }

    public void closeHdfs() throws IOException {
        if (hdfs != null) hdfs.close();
        hdfs = null;
        LOG.info("HDFS connection is now closed.");
    }

    public String[] listPprlDirectories() throws IOException {
        assertHdfsOpen();
        assertBasePathExists();
        FileStatus[] fss = hdfs.listStatus(pprlBasePath);
        String[] ss = new String[fss.length];
        int i = 0;
        for (FileStatus fs : fss) {
            ss[i] = fs.getPath() + "\t" + fs.getOwner() + ":" + fs.getGroup() + "\t" + fs.getPermission();
            i++;
        }
        return ss;
    }

    public void makePprlPartyPaths(final String linkingParty, final String[] dataParties) throws IOException {
        assertHdfsOpen();
        if (!hdfs.exists(pprlBasePath)) {
            makePprlBasePath(linkingParty);
            LOG.info("Base directory does not exist on the HDFS site. Creating {}", pprlBasePath);
        }
        if (linkingParty != null) {
            makePprlPartyPath(linkingParty);
        }
        if (dataParties != null) {
            for (String party : dataParties) makePprlPartyPath(party);
        }
    }

    public void makePprlBasePath(final String linkingParty) throws IOException {
        assertHdfsOpen();
        hdfs.mkdirs(pprlBasePath, basePathPermissions);
        hdfs.setOwner(pprlBasePath, linkingParty, linkingParty);
    }

    public void makePprlPartyPath(final String party) throws IOException {
        assertHdfsOpen();
        final Path dataPartyPath = getPprlPartyPath(party);
        assertBasePathExists();
        hdfs.mkdirs(dataPartyPath, partyPathPermissions);
        hdfs.setOwner(dataPartyPath, party, party);
        LOG.info("Creating {} with permissions {} and owner {}:{}", dataPartyPath, partyPathPermissions,
                party, party);
    }

    public void deletePprlBasePath() throws IOException {
        assertHdfsOpen();
        assertBasePathExists();
        hdfs.delete(pprlBasePath, true);
        LOG.info("Deleting base directory .");
    }

    public void deletePprlPartyPath(final String party) throws IOException {
        assertHdfsOpen();
        assertBasePathExists();
        final Path partyPath = getPprlPartyPath(party);
        hdfs.delete(partyPath, true);
    }

    public boolean pprlBasePathExists() throws IOException {
        assertHdfsOpen();
        return hdfs.exists(pprlBasePath);
    }

    public boolean isUserRestricted() {
        return withPermissions;
    }

    public String getLinkingParty() throws IOException {
        assertHdfsOpen();
        assertBasePathExists();
        return hdfs.getFileStatus(pprlBasePath).getOwner();
    }

    public String[] getDataParties() throws IOException {
        assertHdfsOpen();
        assertBasePathExists();
        final String linkingParty = getLinkingParty();
        FileStatus[] fss = hdfs.listStatus(pprlBasePath);
        final String[] ss = new String[fss.length - 1];
        int i = 0;
        for (FileStatus fs : fss) {
            if (fs.getOwner().equals(linkingParty))
                continue;
            ss[i] = fs.getOwner();
            i++;
        }
        return ss;
    }

    public boolean isPartyLinkingParty(final String party) throws IOException {
        assertHdfsOpen();
        assertBasePathExists();
        return hdfs.getFileStatus(pprlBasePath).getOwner().equals(party) &&
                hdfs.getFileStatus(pprlBasePath).getGroup().equals(party) &&
                hdfs.exists(getPprlPartyPath(party)) &&
                hdfs.getFileStatus(getPprlPartyPath(party)).getOwner().equals(party) &&
                hdfs.getFileStatus(getPprlPartyPath(party)).getGroup().equals(party);

    }

    public boolean isPartyDataParty(final String party) throws IOException {
        assertHdfsOpen();
        assertBasePathExists();
        boolean isNotLinkingParty = !isPartyLinkingParty(party);
        return isNotLinkingParty &&
                hdfs.exists(getPprlPartyPath(party)) &&
                hdfs.getFileStatus(getPprlPartyPath(party)).getOwner().equals(party) &&
                hdfs.getFileStatus(getPprlPartyPath(party)).getGroup().equals(party);
    }

    public void uploadFile(final String party, final String localFileName) throws IOException {
        assertHdfsOpen();
        assertBasePathExists();
        File localFile = new File(localFileName);
        Path source = new Path(localFile.toURI());
        Path destination = new Path(getPprlPartyPath(party) + "/" + localFile.getName());
        hdfs.copyFromLocalFile(source, destination);
        hdfs.setPermission(destination, uploadedFilesPermissions);
        LOG.info("File \"{}\" uploaded at {} on HDFS.", localFileName, destination);
    }

    public String[] listUploadedFiles(final String party) throws IOException {
        assertHdfsOpen();
        assertBasePathExists();
        FileStatus[] fss = hdfs.listStatus(getPprlPartyPath(party));
        final String[] ss = new String[fss.length];
        for (int i = 0; i < fss.length; i++) {
            ss[i] = fss[i].getPath() + "\t" + fss[i].getOwner() + ":" + fss[i].getGroup() + "\t" + fss[i].getPermission();
        }
        LOG.info("Returning files of party  \"{}\" directory on HDFS : {} .", party, getPprlPartyPath(party));
        return ss;
    }

    public void deleteUploadedFile(final String party, final String remoteFileName) throws IOException {
        assertHdfsOpen();
        assertBasePathExists();
        final Path remoteFilePath = new Path(getPprlPartyPath(party) + "/" + remoteFileName);
        if (!hdfs.exists(remoteFilePath)) throw new IOException("file \"" + remoteFileName +
                "\" does not exist on " + getPprlPartyPath(party));
        hdfs.delete(remoteFilePath, true);
        LOG.info("File \"{}\" uploaded at {} on HDFS.", remoteFileName, remoteFilePath);
    }

    public FileSystem getHdfs() {
        return hdfs;
    }

    public Path getPprlBasePath() {
        return pprlBasePath;
    }
}