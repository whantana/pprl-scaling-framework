package gr.upatras.ceid.pprl.shell.provider;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.shell.support.util.FileUtils;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ShellBannerProvider extends DefaultBannerProvider {

    @Value("${user.name}")
    private String username;

    @Value("${hadoop.host}")
    private String hadoopHost;

    @Value("${build.version}")
    private String buildVersion;

    @Value("${build.date}")
    private String buildDate;


    @Value("${spring.profiles.active}")
    private String springProfile;

    public String getBanner() {
        return FileUtils.readBanner(ShellBannerProvider.class, "/banner.txt") + "\n" + getVersion() +
                OsUtils.LINE_SEPARATOR + OsUtils.LINE_SEPARATOR;
    }

    public String getVersion() {
        return String.format("version:%s | build date:%s | profile:%s",buildVersion,
                buildDate,springProfile);
	}

	public String getWelcomeMessage() {
        return "Welcome \"" + username + "\" to PPRL Framework CLI. " +
                "\nPPRL Hadoop Site : "+ hadoopHost + ". You can type \'help\' to get started.";
    }
}
