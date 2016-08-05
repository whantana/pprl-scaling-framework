package gr.upatras.ceid.pprl.mapreduce.input;

import org.apache.commons.io.Charsets;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DblpCharMapping {

    private final static String REGEX = "(&[a-zA-Z]*;)";
    private final static Pattern PATTERN = Pattern.compile(REGEX);

    public static String unescapeXMLChars(String value) {
        if (!value.contains("&") || !value.contains(";"))
            return value;

        Matcher m = PATTERN.matcher(value);
        while (m.find()) {
            String foundKey = m.group();
            String key = foundKey.substring(1, foundKey.length() - 1);
            if(map.containsKey(key)) {
                final String newKey = Charsets.ISO_8859_1.decode(
                        ByteBuffer.wrap(new byte[]{map.get(key)})).toString();
                value = value.replace(foundKey, newKey);
            }
        }
        return value;
    }

    public static Map<String,Byte> map = new HashMap<>();
    static {
        map.put("Agrave", (byte) 192);
        map.put("Aacute", (byte) 193);
        map.put("Acirc", (byte) 194);
        map.put("Atilde", (byte) 195);
        map.put("Auml", (byte) 196);
        map.put("Aring", (byte) 197);
        map.put("AElig", (byte) 198);
        map.put("Ccedil", (byte) 199);
        map.put("Egrave", (byte) 200);
        map.put("Eacute", (byte) 201);
        map.put("Ecirc", (byte) 202);
        map.put("Euml", (byte) 203);
        map.put("Igrave", (byte) 204);
        map.put("Iacute", (byte) 205);
        map.put("Icirc", (byte) 206);
        map.put("Iuml", (byte) 207);
        map.put("ETH",(byte) 208);
        map.put("Ntilde", (byte) 209);
        map.put("Ograve", (byte) 210);
        map.put("Oacute", (byte) 211);
        map.put("Ocirc", (byte) 212);
        map.put("Otilde", (byte) 213);
        map.put("Ouml", (byte) 214);
        map.put("Oslash", (byte) 216);
        map.put("Ugrave", (byte) 217);
        map.put("Uacute", (byte) 218);
        map.put("Ucirc", (byte) 219);
        map.put("Uuml", (byte) 220);
        map.put("Yacute", (byte) 221);
        map.put("THORN", (byte) 222);
        map.put("szlig", (byte) 223);
        map.put("agrave", (byte) 224);
        map.put("aacute", (byte) 225);
        map.put("acirc", (byte) 226);
        map.put("atilde", (byte) 227);
        map.put("auml", (byte) 228);
        map.put("aring", (byte) 229);
        map.put("aelig", (byte) 230);
        map.put("ccedil", (byte) 231);
        map.put("egrave", (byte) 232);
        map.put("eacute", (byte) 233);
        map.put("ecirc", (byte) 234);
        map.put("euml", (byte) 235);
        map.put("igrave", (byte) 236);
        map.put("iacute", (byte) 237);
        map.put("icirc", (byte) 238);
        map.put("iuml", (byte) 239);
        map.put("eth", (byte) 240);
        map.put("ntilde", (byte) 241);
        map.put("ograve", (byte) 242);
        map.put("oacute", (byte) 243);
        map.put("ocirc", (byte) 244);
        map.put("otilde", (byte) 245);
        map.put("ouml", (byte) 246);
        map.put("oslash", (byte) 248);
        map.put("ugrave", (byte) 249);
        map.put("uacute", (byte) 250);
        map.put("ucirc", (byte) 251);
        map.put("uuml", (byte) 252);
        map.put("yacute", (byte) 253);
        map.put("thorn", (byte) 254);
        map.put("yuml", (byte) 255);
    }
}
