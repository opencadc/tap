package ca.nrc.cadc.tap.schema.validator;

public class Violation {

    public enum Severity {
        ERROR, WARNING
    }

    private final Severity severity;
    private final ViolationType violationType;
    private final String message;

    // TODO: check for setting severity
    public Violation(Severity severity, ViolationType violationType, String message) {
        this.severity = severity;
        this.violationType = violationType;
        this.message = message;
    }

    public Severity getSeverity() {
        return severity;
    }

    public ViolationType getViolationType() {
        return violationType;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "[" + severity + "] (" + violationType + ") " + message;
    }

}
