package ca.nrc.cadc.tap.schema.validator;

public class Violation {

    private final ViolationType violationType;
    private final String message;

    public Violation(ViolationType violationType, String message) {
        this.violationType = violationType;
        this.message = message;
    }

    public ViolationType getViolationType() {
        return violationType;
    }

    public String getMessage() {
        return message;
    }

}
