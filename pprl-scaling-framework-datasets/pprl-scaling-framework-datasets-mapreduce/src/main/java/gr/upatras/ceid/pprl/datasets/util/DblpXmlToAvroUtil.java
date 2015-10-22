package gr.upatras.ceid.pprl.datasets.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DblpXmlToAvroUtil {

    public static String shortenUrl(final String url) {
        Pattern p = Pattern.compile(".*://.*?(/.*)");
        Matcher m = p.matcher(url);
        if(m.matches()) {
            return m.group(1);
        } else {
            p = Pattern.compile(".*?(/.*)");
            m = p.matcher(url);
            if(m.matches()) return m.group(1);
            else return url;
        }
    }
}
