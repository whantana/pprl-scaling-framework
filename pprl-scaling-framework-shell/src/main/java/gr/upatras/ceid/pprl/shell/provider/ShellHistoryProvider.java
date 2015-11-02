package gr.upatras.ceid.pprl.shell.provider;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultHistoryFileNameProvider;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ShellHistoryProvider extends DefaultHistoryFileNameProvider {

	@Value("${user.name}")
	private String username;

	public String getHistoryFileName() {
		return "." + username + ".pprl.shell.log";
	}
}

