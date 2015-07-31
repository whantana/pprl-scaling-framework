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

    public String getBanner() {
        return FileUtils.readBanner(ShellBannerProvider.class, "/banner.txt") + "\n" + getVersion() +
                OsUtils.LINE_SEPARATOR + OsUtils.LINE_SEPARATOR;
    }


	public String getVersion() {
		return "1.0.0";
	}

	public String getWelcomeMessage() {
        return "Welcome \"" + username + "\" to PPRL Framework CLI. You can type \'help\' to get started.";
    }
}
