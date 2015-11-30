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
		Bootstrap.main(args);
	}

	// TODO shell has functionalities that do not need a hadoop cluster.
	// TODO Initialize app without the hadoop instance
}