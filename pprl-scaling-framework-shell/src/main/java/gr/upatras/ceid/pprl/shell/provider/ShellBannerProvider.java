package gr.upatras.ceid.pprl.shell.provider;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

    @Value("${build.version}")
    private String buildVersion;

    @Value("${build.date}")
    private String buildDate;

    @Value("${user.dir}")
    private String userDirectory;

    @Autowired
    @Qualifier("isClusterReady")
    private Boolean isClusterReady;


    public String getBanner() {
        return FileUtils.readBanner(ShellBannerProvider.class, "/banner.txt") + "\n" + getVersion() +
                OsUtils.LINE_SEPARATOR + OsUtils.LINE_SEPARATOR;
    }

    public String getVersion() {
        return String.format("version:%s | build date:%s ",buildVersion, buildDate);
	}

	public String getWelcomeMessage() {
        final StringBuilder sb = new StringBuilder("Welcome \"" + username + "\" to PPRL Framework CLI.");
        sb.append("\n---\nYou can type \'help\' to get started.");
        sb.append(String.format("\n--\nUser directory : %s", userDirectory));
        if (!isClusterReady) return sb.toString();
        sb.append("\n--\nClient is hadoop-ready. Look for config.log");
        return sb.toString();
    }
}
