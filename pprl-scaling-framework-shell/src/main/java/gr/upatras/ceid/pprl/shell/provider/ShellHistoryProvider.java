package gr.upatras.ceid.pprl.shell.provider;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultHistoryFileNameProvider;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ShellHistoryProvider extends DefaultHistoryFileNameProvider {

	public String getHistoryFileName() {
		return "pprl.shell.log";
	}

	@Override
	public String getProviderName() {
		return "PPRL.framework.shell.log";
	}
}

