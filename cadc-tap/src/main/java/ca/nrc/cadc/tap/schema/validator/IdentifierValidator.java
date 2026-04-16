package ca.nrc.cadc.tap.schema.validator;

import ca.nrc.cadc.tap.schema.validator.adql.ReservedKeyword;

import java.util.ArrayList;
import java.util.List;

public class IdentifierValidator {

    public enum IdentifierType {
        SCHEMA_NAME, TABLE_NAME, COLUMN_NAME
    }

    /**
     * Validates that the given String is a valid ADQL identifier.
     *
     * @param identifier String to be tested.
     */
    public List<Violation> checkValidIdentifier(String identifier, IdentifierType identifierType) {

        List<Violation> violations = new ArrayList<>();

        // identifier shouldn't be null and cannot be an empty string.
        if (identifier == null || identifier.trim().isEmpty()) {
            violations.add(new Violation(ViolationType.NULL_OR_BLANK, "Identifier is null or empty"));
            return violations;
        }

        // identifier cannot start with, contain, or end, with a space.
        if (identifier.startsWith(" ") || identifier.contains(" ") || identifier.endsWith(" ")) {
            violations.add(new Violation(ViolationType.STRUCTURAL, "Identifier contains spaces"));
            identifier = identifier.replaceAll(" ", "");
        }

        if (identifierType.equals(IdentifierType.COLUMN_NAME) && identifier.startsWith("\"") && identifier.endsWith("\"")) {
            violations.add(new Violation(ViolationType.IDENTIFIER_QUOTED, "Identifier is double quoted"));
            identifier = identifier.substring(1, identifier.length() - 1);
        }

        // identifier must start with a letter {aA-zZ}.
        if (!isAsciiLetter(identifier.charAt(0))) {
            violations.add(new Violation(ViolationType.STRUCTURAL, "Identifier must start with a letter"));
        }

        // subsequent characters must be letters, underscores, or digits.
        for (int i = 1; i < identifier.length(); i++) {
            if (!isValidIdentifierCharacter(identifier.charAt(i))) {
                violations.add(new Violation(ViolationType.IDENTIFIER_INVALID_CHAR, "Identifier contains an invalid character " + identifier.charAt(i)));
            }
        }

        // Identifier cannot be a reserved keyword.
        if (ReservedKeyword.isReserved(identifier)) {
            violations.add(new Violation(ViolationType.IDENTIFIER_RESERVED_KEYWORD, "Identifier '" + identifier + "' is a reserved keyword."));
        }

        return violations;
    }

    /**
     * Checks if the char is a valid US ASCII letter.
     *
     * @param c char to test.
     * @return true if the char is valid ASCII, false otherwise.
     */
    public boolean isAsciiLetter(char c) {
        return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z';
    }

    /**
     * Checks if the char is either a US ASCII char, an underscore,
     * of a digit.
     *
     * @param c char to test.
     * @return true if the char is a valid ADQL identifier char, false otherwise.
     */
    public boolean isValidIdentifierCharacter(char c) {
        return c == '_' || isAsciiLetter(c) || Character.isDigit(c);
    }

    public List<Violation> checkValidTableName(String identifier) {
        List<Violation> violations = new ArrayList<>();
        String[] parts = identifier.split("[.]");
        String schemaName = null;
        String tableName = identifier;
        if (parts.length == 2) {
            schemaName = parts[0];
            tableName = parts[1];
        } else if (parts.length > 2) {
            violations.add(new Violation(ViolationType.STRUCTURAL, "invalid table name: " + identifier + " (too many parts)"));
            return violations;
        }

        if (schemaName != null) {
            violations.addAll(checkValidIdentifier(schemaName, IdentifierType.SCHEMA_NAME));
        }

        violations.addAll(checkValidIdentifier(tableName, IdentifierType.TABLE_NAME));
        return violations;
    }

}
