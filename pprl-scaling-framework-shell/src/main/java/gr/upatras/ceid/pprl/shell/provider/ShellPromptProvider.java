package gr.upatras.ceid.pprl.shell.provider;


import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultPromptProvider;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ShellPromptProvider extends DefaultPromptProvider {

    @Override
    public String getPrompt() {
        return "pprl-shell>";
    }


    @Override
    public String getProviderName() {
        return "PPRL.Framework.prompt";
    }
}
