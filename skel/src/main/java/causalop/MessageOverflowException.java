package causalop;

public class MessageOverflowException extends RuntimeException {
    public MessageOverflowException() {
        super();
    }

    public MessageOverflowException(String message) {
        super(message);
    }
}
