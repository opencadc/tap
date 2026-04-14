package ca.nrc.cadc.tap.schema.validator;

public enum ViolationType {

    // --- structural (always errors, cannot be downgraded) ---
    NULL_OR_BLANK,
    STRUCTURAL,

    // --- UCD ---
    UCD_UNKNOWN_WORD, // word not in IVOA controlled vocabulary
    UCD_DEPRECATED_WORD, // word is in vocabulary but deprecated
    UCD_PRIMARY_POSITION, // S-only word used as primary
    UCD_SECONDARY_POSITION, // P-only word used as secondary

    // --- VOUnit ---
    VOUNIT_QUOTED_IDENTIFIER, // e.g. 'furlong'
    VOUNIT_UNKNOWN_UNIT, // unrecognised base unit
    VOUNIT_UNKNOWN_FUNCTION,
    VOUNIT_CASE_SENSITIVE, // e.g. 'Hz' vs 'hz'

}
