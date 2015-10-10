package gr.upatras.ceid.pprl.shell.provider;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultPromptProvider;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ShellPromptProvider extends DefaultPromptProvider {

    @Value("${user.name}")
    private String username;

    @Override
    public String getPrompt() {
        return username + ".pprl-shell> ";
    }
}
