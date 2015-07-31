import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DatasetCommandsTest {

    @Test
    public void test1() {
        String okString = "pprl";
        String notOkString = "p@.prl";
        assertFalse(Pattern.compile("\\W").matcher(okString).find());
        assertTrue(Pattern.compile("\\W").matcher(notOkString).find());
    }
}
