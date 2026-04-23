package ca.nrc.cadc.tap.schema.validator;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Decides which {@link ViolationType}s are treated as errors (failing).
 * Kinds not in the error set are downgraded to warnings.
 */
public class ValidatorConfig {

    public enum Severity {
        ERROR, WARNING
    }

    public enum ConfigType {
        STRICT, LAX, NONE
    }

    private final ConfigType configType;

    /** Kinds that are structurally mandatory — can never be warnings. */
    private static final Set<ViolationType> ALWAYS_ERRORS = Collections.unmodifiableSet(EnumSet.of(
            ViolationType.NULL_OR_BLANK,
            ViolationType.IDENTIFIER_INVALID_CHAR,
            ViolationType.IDENTIFIER_RESERVED_KEYWORD // allowing quoted identifiers if config is not "strict"
    ));

    private final Set<ViolationType> errorTypes;

    private ValidatorConfig(Set<ViolationType> errorTypes, ConfigType configType) {
        EnumSet<ViolationType> merged = EnumSet.copyOf(ALWAYS_ERRORS);
        merged.addAll(errorTypes);
        this.errorTypes = Collections.unmodifiableSet(merged);
        this.configType = configType;
    }

    // factories

    /** Every Violation is an error — full IVOA compliance required. */
    public static ValidatorConfig strict() {
        return new ValidatorConfig(EnumSet.allOf(ViolationType.class), ConfigType.STRICT);
    }

    /** Default configuration.
     *  A set including ALWAYS_ERRORS and below listed are considered errors.*/
    public static ValidatorConfig lax() {
        return new ValidatorConfig(EnumSet.of(
                ViolationType.STRUCTURAL,
                ViolationType.UCD_UNKNOWN_WORD,
                ViolationType.UCD_POSITION_MISMATCH
        ), ConfigType.LAX);
    }

    /** Only ALWAYS_ERRORS set is considered errors. */
    public static ValidatorConfig none() {
        return new ValidatorConfig(Collections.emptySet(), ConfigType.NONE);
    }

    public ConfigType getConfigType() {
        return configType;
    }

    public boolean isError(ViolationType violationType) {
        return errorTypes.contains(violationType);
    }

    public Severity severityFor(ViolationType violationType) {
        return isError(violationType) ? Severity.ERROR : Severity.WARNING;
    }
}
