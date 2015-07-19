package gr.upatras.ceid.pprl.shell.provider;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ShellBannerProvider extends DefaultBannerProvider {

    public String getBanner() {
   		StringBuffer buf = new StringBuffer();
   		buf.append(" __   __   __           ___  __              ___       __   __").append(OsUtils.LINE_SEPARATOR);
   		buf.append("|__) |__) |__) |       |__  |__)  /\\   |\\/| |__  |  | /  \\ |__) |__/").append(OsUtils.LINE_SEPARATOR);
   		buf.append("|    |    |  \\ |___    |    |  \\ /~~\\  |  | |___ |/\\| \\__/ |  \\ |  \\").append(OsUtils.LINE_SEPARATOR);
   		buf.append("Version:").append(this.getVersion());
   		return buf.toString();
   	}

	public String getVersion() {
		return "1.0.0";
	}

	public String getWelcomeMessage() {
		return "Welcome to PPRL Framework CLI";
	}

	@Override
	public String getProviderName() {
		return "PPRL.Framework.Banner";
	}
}
