package gr.upatras.ceid.pprl.shell;

import org.springframework.shell.Bootstrap;

import java.io.IOException;

public class Main {

	/**
	 * Main class that delegates to Spring Shell's Bootstrap class in order to simplify debugging inside an IDE
	 *
	 * @param args arguments
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO get home dir with kerberos credentials
		// before starting shell a check up with the cluster is needed
		Bootstrap.main(args);
	}
}