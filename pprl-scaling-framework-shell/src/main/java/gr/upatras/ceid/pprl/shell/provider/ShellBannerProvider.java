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
    // TODO injected so far . Kerberos credentials must be provided here too
    @Value("${user.name}")
    private String username;

    @Value("${hadoop.host}")
    private String hadoopHost;

    @Value("${hive.host}")
    private String hiveHost;

    @Value("${spring.profiles.active}")
    private String springProfile;

    public String getBanner() {
        return FileUtils.readBanner(ShellBannerProvider.class, "/banner.txt") + "\n" + getVersion() +
                OsUtils.LINE_SEPARATOR + OsUtils.LINE_SEPARATOR;
    }

    public String getVersion() {
		return "1.0.0" + springProfile;
	} // TODO do proper version from maven "MAJOR.MINOR-build-profile

	public String getWelcomeMessage() {
        return "Welcome \"" + username + "\" to PPRL Framework CLI. " +
                "\nHadoop Host : "+ hadoopHost + ", Hive Host : " + hiveHost + ". You can type \'help\' to get started.";
    }
}
