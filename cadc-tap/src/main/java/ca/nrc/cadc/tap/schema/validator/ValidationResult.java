package ca.nrc.cadc.tap.schema.validator;

import java.util.Collections;
import java.util.List;

public class ValidationResult {

    private final String input;
    private final boolean valid; // Constructor sets it to true if there is no violation of type ERROR
    private final List<Violation> violations;

    public ValidationResult(String input, List<Violation> violations, ValidatorConfig config) {
        this.input = input;
        this.violations = Collections.unmodifiableList(violations);
        this.valid = violations.stream().noneMatch(v -> config.severityFor(v.getViolationType()) == ValidatorConfig.Severity.ERROR);
    }

    public String getInput() {
        return input;
    }

    public boolean isValid() {
        return valid;
    }

    public List<Violation> getViolations() {
        return violations;
    }

}