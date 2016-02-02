package gr.upatras.ceid.pprl.shell.provider;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultPromptProvider;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ShellPromptProvider extends DefaultPromptProvider {

    @Value("${user.name}")
    private String username;

    @Value("${user.dir}")
    private String userDirectory;

    @Override
    public String getPrompt() {
        return String.format("%s.%s $ ",
                username,
                userDirectory.substring(userDirectory.lastIndexOf(File.separatorChar)));
    }
}
