package gr.upatras.ceid.pprl.blocking;

/**
 * Blocking exception class.
 */
public class BlockingException extends Exception {
    /**
     * Exception constructor.
     *
     * @param reason reason of exception.
     */
    public BlockingException(final String reason) { super(reason);}
}
