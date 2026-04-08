package ca.nrc.cadc.tap.schema.validator;

public class Violation {

    public enum Severity {
        ERROR, WARNING
    }

    private final Severity severity;
    private final String message;

    public Violation(Severity severity, String message) {
        this.severity = severity;
        this.message = message;
    }

    public Severity getSeverity() {
        return severity;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "[" + severity + "] " + message;
    }

}
