package gr.upatras.ceid.pprl.shell;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.shell.Bootstrap;

import java.io.IOException;

@Configuration
@ComponentScan(
   value = { "gr.upatras.ceid.pprl.shell" },
   basePackages = { "gr.upatras.ceid.pprl.shell.command",
                    "gr.upatras.ceid.pprl.shell.provider"}
)
public class Main {
    /**
   	 * Main class that delegates to Spring Shell's Bootstrap class in order to simplify debugging inside an IDE
   	 * @param args args
   	 * @throws IOException
   	 */
   	public static void main(String[] args) throws IOException {
   		Bootstrap.main(args);
    }
}
