package ca.nrc.cadc.tap.schema.validator;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Decides which {@link ViolationType}s are treated as errors (failing).
 * Kinds not in the error set are downgraded to warnings.
 */
public class ValidatorConfig {

    /** Kinds that are structurally mandatory — can never be warnings. */
    private static final Set<ViolationType> ALWAYS_ERRORS = Collections.unmodifiableSet(
            EnumSet.of(ViolationType.NULL_OR_BLANK, ViolationType.STRUCTURAL));

    private final Set<ViolationType> errorKinds;

    private ValidatorConfig(Set<ViolationType> errorKinds) {
        EnumSet<ViolationType> merged = EnumSet.copyOf(ALWAYS_ERRORS);
        merged.addAll(errorKinds);
        this.errorKinds = Collections.unmodifiableSet(merged);
    }

    // factories

    /** Everything is an error — full IVOA compliance required. */
    public static ValidatorConfig pedantic() {
        return new ValidatorConfig(EnumSet.allOf(ViolationType.class));
    }

    /** Only NULL_OR_BLANK and STRUCTURAL are errors. */
    public static ValidatorConfig strict() {
        return new ValidatorConfig(Collections.emptySet()); //
    }

    /**
     * Default Configuration
     */
    public static ValidatorConfig defaultConfig() {
        return new ValidatorConfig(EnumSet.of(
                ViolationType.NULL_OR_BLANK,
                ViolationType.STRUCTURAL,
                ViolationType.UCD_UNKNOWN_WORD,
                ViolationType.UCD_PRIMARY_POSITION,
                ViolationType.UCD_SECONDARY_POSITION));
    }

    /** Custom - Explicitly specify which violationTypes should be errors. */
    public static ValidatorConfig of(ViolationType... violationTypes) {
        return new ValidatorConfig(EnumSet.copyOf(java.util.Arrays.asList(violationTypes)));
    }

    public boolean isError(ViolationType violationType) {
        return errorKinds.contains(violationType);
    }

    public Violation.Severity severityFor(ViolationType violationType) {
        return isError(violationType) ? Violation.Severity.ERROR : Violation.Severity.WARNING;
    }
}
