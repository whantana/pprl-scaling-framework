package gr.upatras.ceid.pprl.datasets.command;

import gr.upatras.ceid.pprl.datasets.service.HdfsPprlService;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

@Component
public class DatasetsCommands implements CommandMarker {

    /*
     * Datasets Commands :
     *
     * all users can :
     * ds_hdfs open. Open hdfs connection. Also identify if user is linking party, data party or none.
     * ds_hdfs close. Close the hdfs connection.
     * ds_hdfs print_directories. Print all directories in the pprl base path on HDFS.
     * ds_hdfs print_parties. Print all parties (linking and data).
     *
     *
     * hdfs super-user can :
     * ds_hdfs make_directories. Create the directory schema.
     * ds_hdfs delete_directories. Delete the directories schema.
     *
     * parties can :
     * ds_hdfs upload_file. Upload a file on the party directory
     * ds_hdfs list_uploaded_files. List uploaded files on the party directory.
     * ds_hdfs delete_file. Delete a file on the party directory.
     * ds_hdfs delete_directory. Delete the directory.
     */

    @Autowired
    private HdfsPprlService service;

    @Value("${user.name}")
    private String username;

    private boolean isHdfsOpen = false;
    private boolean pprlBasePathExists = false;
    private boolean isUserSuperUser = false;
    private boolean isUserPprlParty = false;
    private boolean isUserLinkingParty = false;
    private boolean isUserDataParty = false;


    @CliAvailabilityIndicator({"ds_hdfs open"})
    public boolean dsHdfsOpenAvailability() {
        return !isHdfsOpen;
    }

    @CliAvailabilityIndicator({"ds_hdfs close"})
    public boolean dsHdfsCloseAvailability() {
        return isHdfsOpen;
    }

    @CliAvailabilityIndicator({"ds_hdfs print_directories", "ds_hdfs print_parties"})
    public boolean dsHdfsPrintDirectoriesAndPartiesAvailability() {
        return isHdfsOpen && pprlBasePathExists;
    }

    @CliAvailabilityIndicator({"ds_hdfs make_directories"})
    public boolean dsHDfsMakeDirectoriesAvailability() {
        return isHdfsOpen && isUserSuperUser && !pprlBasePathExists;
    }

    @CliAvailabilityIndicator({"ds_hdfs delete_directories"})
    public boolean dsHDfsDeleteDirectoriesAvailability() {
        return isHdfsOpen && isUserSuperUser && pprlBasePathExists;
    }

    @CliAvailabilityIndicator({"ds_hdfs upload_file", "ds_hdfs list_uploaded_files", "ds_hdfs delete_file", "ds_hdfs delete_directory"})
    public boolean dsHdfsUploadListDeleteFilesAvailability() {
        return isHdfsOpen && pprlBasePathExists && isUserPprlParty;
    }

    private void reset() {
        isHdfsOpen = false;
        pprlBasePathExists = false;
        isUserSuperUser = false;
        isUserPprlParty = false;
        isUserLinkingParty = false;
        isUserDataParty = false;
    }

    @CliCommand(value = "ds_hdfs open", help = "Open connection with configured HDFS site.")
    public String dsHdfsOpenCommand() {
        StringBuilder sb = new StringBuilder("HDFS connection is now open.");
        try {
            service.openHdfs();
            isHdfsOpen = true;
            pprlBasePathExists = service.pprlBasePathExists();
            sb.append(pprlBasePathExists ? "PPRL directories found at :" +
                    service.getPprlBasePath() + " ." : "PPRL directories NOT found.");
            isUserSuperUser =
                    service.getHdfs().getFileStatus(new Path("/")).getOwner().equals(username);
            if (isUserSuperUser) sb.append(username).append(" is HDFS super-user.");

            if (pprlBasePathExists) {
                isUserLinkingParty = service.isPartyLinkingParty(username);
                if (isUserLinkingParty) sb.append(username).append(" is PPRL Linking Party.");
                isUserDataParty = service.isPartyDataParty(username);
                if (isUserDataParty) sb.append(username).append(" is PPRL Data Party.");
            }
            isUserPprlParty = isUserDataParty | isUserLinkingParty;
        } catch (IOException ioe) {
            reset();
            return "Exception occured : " + ioe.getMessage();
        }

        return sb.toString();
    }

    @CliCommand(value = "ds_hdfs close", help = "Close connection with configured HDFS site.")
    public String dsHdfsCloseCommand() {
        try {
            service.closeHdfs();
            reset();
        } catch (IOException ioe) {
            return "Exception occured : " + ioe.getMessage();
        }
        return "HDFS connection is now closed.";
    }

    @CliCommand(value = "ds_hdfs print_directories", help = "Print all directories in the pprl base path on HDFS.")
    public String dsHdfsPrintDirectoriesCommand() {
        StringBuilder sb = new StringBuilder("PPRL Directories (base=" + service.getPprlBasePath() + "). ");
        try {
            sb.append("Linking Party : ").append(service.getLinkingParty()).append(". Data Parties : ")
                    .append(Arrays.toString(service.getDataParties())).append(". Listing :\n");
            int i = 1;
            for (String s : service.listPprlDirectories()) {
                sb.append(i).append(" ").append(s).append("\n");
                i++;
            }
        } catch (IOException e) {
            return "Exception occured : " + e.getMessage();
        }
        return sb.toString();
    }

    @CliCommand(value = "ds_hdfs print_parties", help = "Print all parties.")
    public String dsHdfsPrintPartiesCommand() {
        StringBuilder sb = new StringBuilder("Linking Party : ");
        try {
            sb.append(service.getLinkingParty()).append(".\nData Parties : ")
                    .append(Arrays.toString(service.getDataParties()));
        } catch (IOException e) {
            return "Exception occured : " + e.getMessage();
        }
        return sb.toString();
    }

    @CliCommand(value = "ds_hdfs make_directories", help = "Create the directory structure from input parties.")
    public String dsHdfsMakeDirectoriesCommand(
            @CliOption(key = {"linking_party"}, mandatory = true, help = "A Linking party name.") final String linkingParty,
            @CliOption(key = {"data_parties"}, mandatory = true, help = "Comma-separated Data party names.") final String dataParties) {

        if ((linkingParty != null && !Pattern.compile("\\W").matcher(linkingParty).find()) &&
                (dataParties != null && dataParties.contains(","))) {
            String[] dataPartiesArray = dataParties.split(",");
            for (String party : dataPartiesArray)
                if (Pattern.compile("\\W").matcher(party).find())
                    return "Parties input seems invalid. Aborting.";

            try {
                service.makePprlPartyPaths(linkingParty, dataPartiesArray);
                isUserLinkingParty = service.isPartyLinkingParty(username);
                isUserDataParty = service.isPartyDataParty(username);
                isUserPprlParty = isUserDataParty | isUserLinkingParty;
            } catch (IOException e) {
                return "Exception occured : " + e.getMessage();
            }
            pprlBasePathExists = true;
            return "Done making PPRL directories on HDFS site.";
        }
        return "Parties input seems invalid. Aborting.";
    }

    @CliCommand(value = "ds_hdfs delete_directories", help = "Delete the directory structure.")
    public String dsHdfsDeleteDirectoriesCommand() {
        try {
            service.deletePprlBasePath();
        } catch (Exception e) {
            return "Exception occured : " + e.getMessage();
        }
        pprlBasePathExists = false;
        return "Done deleting PPRL directories on HDFS site.";
    }

    @CliCommand(value = "ds_hdfs upload_file", help = "Upload a file on the party directory.")
    public String dsHdfsUploadFileCommand(
            @CliOption(key = {"file"}, mandatory = true, help = "A local file") final String file) {
        try {
            service.uploadFile(username, file);
        } catch (Exception e) {
            return "Exception occured : " + e.getMessage();
        }
        return "File uploaded for party=\"" + username + "\" on the HDFS site.";
    }

    @CliCommand(value = "ds_hdfs list_uploaded_files", help = "List uploaded files on the party directory.")
    public String dsHdfsListUploadedFilesCommand() {
        StringBuilder sb = new StringBuilder("Uploaded files for party=\"\" + username +\"\" on the HDFS site :\n");
        try {
            final String[] files = service.listUploadedFiles(username);
            int i = 1;
            for (String f : files) {
                sb.append(i).append(" ").append(f).append("\n");
                i++;
            }
        } catch (IOException e) {
            return "Exception occured : " + e.getMessage();
        }
        return sb.toString();
    }

    @CliCommand(value = "ds_hdfs delete_file", help = "Delete a file on the party directory.")
    public String dsHdfsDeleteFileCommand(
            @CliOption(key = {"file"}, mandatory = true, help = "A remote file") final String file) {
        try {
            service.deleteUploadedFile(username, file);
        } catch (IOException e) {
            return "Exception occured : " + e.getMessage();
        }
        return "Deleting the uploaded file=\"" + file + "\" for party=\"" + username + "\" on the HDFS site.";
    }

    @CliCommand(value = "ds_hdfs  delete_directory", help = "Delete the directory.")
    public String dsHdfsDeleteDirectoryCommand() {
        try {
            service.deletePprlPartyPath(username);
        } catch (IOException e) {
            return "Exception occured : " + e.getMessage();
        }
        return "Deleting party directory for party=\"" + username + "\" on the HDFS site.";
    }
}
