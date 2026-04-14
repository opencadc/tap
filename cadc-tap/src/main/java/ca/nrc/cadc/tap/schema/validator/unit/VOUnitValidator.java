/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2026.                            (c) 2026.
 *  Government of Canada                 Gouvernement du Canada
 *  National Research Council            Conseil national de recherches
 *  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 *  All rights reserved                  Tous droits réservés
 *
 *  NRC disclaims any warranties,        Le CNRC dénie toute garantie
 *  expressed, implied, or               énoncée, implicite ou légale,
 *  statutory, of any kind with          de quelque nature que ce
 *  respect to the software,             soit, concernant le logiciel,
 *  including without limitation         y compris sans restriction
 *  any warranty of merchantability      toute garantie de valeur
 *  or fitness for a particular          marchande ou de pertinence
 *  purpose. NRC shall not be            pour un usage particulier.
 *  liable in any event for any          Le CNRC ne pourra en aucun cas
 *  damages, whether direct or           être tenu responsable de tout
 *  indirect, special or general,        dommage, direct ou indirect,
 *  consequential or incidental,         particulier ou général,
 *  arising from the use of the          accessoire ou fortuit, résultant
 *  software.  Neither the name          de l'utilisation du logiciel. Ni
 *  of the National Research             le nom du Conseil National de
 *  Council of Canada nor the            Recherches du Canada ni les noms
 *  names of its contributors may        de ses  participants ne peuvent
 *  be used to endorse or promote        être utilisés pour approuver ou
 *  products derived from this           promouvoir les produits dérivés
 *  software without specific prior      de ce logiciel sans autorisation
 *  written permission.                  préalable et particulière
 *                                       par écrit.
 *
 *  This file is part of the             Ce fichier fait partie du projet
 *  OpenCADC project.                    OpenCADC.
 *
 *  OpenCADC is free software:           OpenCADC est un logiciel libre ;
 *  you can redistribute it and/or       vous pouvez le redistribuer ou le
 *  modify it under the terms of         modifier suivant les termes de
 *  the GNU Affero General Public        la “GNU Affero General Public
 *  License as published by the          License” telle que publiée
 *  Free Software Foundation,            par la Free Software Foundation
 *  either version 3 of the              : soit la version 3 de cette
 *  License, or (at your option)         licence, soit (à votre gré)
 *  any later version.                   toute version ultérieure.
 *
 *  OpenCADC is distributed in the       OpenCADC est distribué
 *  hope that it will be useful,         dans l’espoir qu’il vous
 *  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
 *  without even the implied             GARANTIE : sans même la garantie
 *  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
 *  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
 *  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
 *  General Public License for           Générale Publique GNU Affero
 *  more details.                        pour plus de détails.
 *
 *  You should have received             Vous devriez avoir reçu une
 *  a copy of the GNU Affero             copie de la Licence Générale
 *  General Public License along         Publique GNU Affero avec
 *  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
 *  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
 *                                       <http://www.gnu.org/licenses/>.
 *
 ************************************************************************
 */

package ca.nrc.cadc.tap.schema.validator.unit;

import ca.nrc.cadc.tap.schema.validator.ValidationResult;
import ca.nrc.cadc.tap.schema.validator.ValidatorConfig;
import ca.nrc.cadc.tap.schema.validator.Violation;
import ca.nrc.cadc.tap.schema.validator.ViolationType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.log4j.Logger;

/**
 * Validates VOUnit strings according to the IVOA VOUnits 1.1 standard grammar.
 * Grammar (normative, from the specification):
 * <pre>
 * input                 : complete_expression
 *                       | scalefactor complete_expression
 *                       | LIT1
 *
 * complete_expression   : product_of_units
 *                       | product_of_units division unit_expression
 *
 * product_of_units      : unit_expression
 *                       | product_of_units product unit_expression
 *
 * unit_expression       : term
 *                       | function_application
 *                       | OPEN_P complete_expression CLOSE_P
 *
 * function_application  : STRING OPEN_P function_operand CLOSE_P
 * function_operand      : complete_expression
 *                       | scalefactor complete_expression
 *
 * scalefactor           : LIT10 power numeric_power
 *                       | LIT10
 *                       | LIT1
 *                       | VOUFLOAT
 *
 * division              : DIVISION (i.e. '/')
 * product               : DOT (i.e. '.')
 *
 * term                  : unit
 *                       | unit power numeric_power
 *
 * unit                  : STRING
 *                       | QUOTED_STRING
 *                       | STRING QUOTED_STRING
 *                       | PERCENT
 *
 * power                 : STARSTAR (i.e. '**')
 *
 * numeric_power         : integer
 *                       | parenthesized_number
 *
 * parenthesized_number  : OPEN_P integer CLOSE_P
 *                       | OPEN_P FLOAT CLOSE_P
 *                       | OPEN_P integer division UNSIGNED_INTEGER CLOSE_P
 *
 * integer               : SIGNED_INTEGER | UNSIGNED_INTEGER
 * </pre>
 *
 * @see <a href="https://www.ivoa.net/documents/VOUnits/20231215/REC-VOUnits-1.1.html">
 * IVOA Recommendation: Units in the VO (VOUnits 1.1)</a>
 */
public class VOUnitValidator {

    private static final Logger log = Logger.getLogger(VOUnitValidator.class);

    List<String> functions = Arrays.asList("log", "ln", "exp", "sqrt");

    private final ValidatorConfig config;

    public VOUnitValidator() {
        this(ValidatorConfig.defaultConfig());
    }

    public VOUnitValidator(ValidatorConfig config) {
        this.config = config;
    }

    /**
     * @param vounit the string to validate;
     * @return a {@link ValidationResult} describing the outcome.
     */
    public ValidationResult validate(String vounit) {
        List<Violation> violations = new ArrayList<>();

        if (vounit == null || vounit.trim().isEmpty()) {
            violations.add(new Violation(config.severityFor(ViolationType.NULL_OR_BLANK), ViolationType.NULL_OR_BLANK,
                    "VOUnit string must not be null or blank."));
            return new ValidationResult(null, violations);
        }
        String trimmed = vounit.trim();
        try {
            ParseState s = new ParseState(trimmed);
            parseInput(s, violations);
            if (!s.done()) {
                violations.add(new Violation(config.severityFor(ViolationType.STRUCTURAL), ViolationType.STRUCTURAL,
                        "Unexpected character(s) at position " + s.pos + ": '" + trimmed.substring(s.pos) + "'"));
                return new ValidationResult(vounit, violations);
            }
            return new ValidationResult(vounit, violations);
        } catch (IllegalArgumentException ex) { // TODO: Check on violation kind
            violations.add(new Violation(config.severityFor(ViolationType.STRUCTURAL), ViolationType.STRUCTURAL, ex.getMessage()));
            return new ValidationResult(vounit, violations);
        }
    }

    // ------------------------------------------------------------------
    // Grammar rule: input
    //   input : complete_expression
    //         | scalefactor complete_expression
    //         | LIT1
    // ------------------------------------------------------------------

    private void parseInput(ParseState s, List<Violation> violations) {
        // LIT1 alone ("1") is a valid dimensionless unit
        if ("1".equals(s.input)) {
            s.pos = s.input.length();
            return;
        }

        if ("unknown".equals(s.input) || "UNKNOWN".equals(s.input)) {
            s.pos = s.input.length();
            return;
        }

        // Parse scale factor followed by complete_expression.
        parseScaleFactor(s);
        if (!s.done() && s.current() == ' ') {
            // optional whitespace between a scale factor and units
            s.pos++;
        }
        parseCompleteExpression(s, violations);
    }

    // ------------------------------------------------------------------
    // Grammar rule: complete_expression
    //   complete_expression : product_of_units
    //                       | product_of_units division unit_expression
    // ------------------------------------------------------------------

    private void parseCompleteExpression(ParseState s, List<Violation> violations) {
        parseProductOfUnits(s, violations);

        // Optionally: one division followed by one unit_expression
        if (!s.done() && s.current() == '/') {
            s.pos++; // consume '/'
            parseUnitExpression(s, violations);
        }
    }

    // ------------------------------------------------------------------
    // Grammar rule: product_of_units
    // product_of_units : unit_expression
    //                    | product_of_units product unit_expression
    // product          : DOT  ('.')
    // ------------------------------------------------------------------

    private void parseProductOfUnits(ParseState s, List<Violation> violations) {
        parseUnitExpression(s, violations);
        while (!s.done() && s.current() == '.') {
            s.pos++; // consume '.'
            parseUnitExpression(s, violations);
        }
    }

    // ------------------------------------------------------------------
    // Grammar rule: unit_expression
    //   unit_expression    : term
    //                        | function_application
    //                        | OPEN_P complete_expression CLOSE_P
    // function_application : STRING OPEN_P  function_operand CLOSE_P
    // ------------------------------------------------------------------

    private void parseUnitExpression(ParseState s, List<Violation> violations) {
        if (s.done()) {
            throw new IllegalArgumentException("Unexpected end of input: expected a unit expression.");
        }

        // OPEN_P complete_expression CLOSE_P
        if (s.current() == '(') {
            s.pos++; // consume '('
            parseCompleteExpression(s, violations);
            if (s.done() || s.current() != ')') {
                throw new IllegalArgumentException("Expected ')' at position " + s.pos);
            }
            s.pos++; // consume ')'
            return;
        }

        // function_application or term — both start with a STRING (or QUOTED_STRING or PERCENT).
        // A function_application is STRING '(' ..., so we can look ahead after the symbol.
        // We first try function_application.
        int savedPos = s.pos;
        Token token = matchString(s); // Can have tokenType UNIT, FUNC, UNKNOWN
        if (token != null) {
            int afterSym = s.pos + token.value.length();
            if (afterSym < s.input.length() && s.input.charAt(afterSym) == '(') {
                // This is a function_application: STRING OPEN_P function_operand CLOSE_P
                if (!token.type.equals(TokenType.FUNC)) {
                    violations.add(new Violation(config.severityFor(ViolationType.VOUNIT_UNKNOWN_FUNCTION), ViolationType.VOUNIT_UNKNOWN_FUNCTION,
                            "Expected a function name. Unknown function : " + token.value + " found at position " + s.pos));
                }
                s.pos += token.value.length(); // consume function name
                s.pos++; // consume '('
                parseFunctionOperand(s, violations);
                if (s.done() || s.current() != ')') {
                    throw new IllegalArgumentException("Expected ')' to close function '" + token + "' at position " + s.pos);
                }
                s.pos++; // consume ')'
                return;
            }
        }
        // Not a function – parse as term
        s.pos = savedPos;
        parseTerm(s, violations);
    }

    // ------------------------------------------------------------------
    // Grammar rule: function_operand
    //   function_operand : complete_expression
    //                    | scalefactor complete_expression
    // ------------------------------------------------------------------

    private void parseFunctionOperand(ParseState s, List<Violation> violations) {
        parseScaleFactor(s);
        if (!s.done() && s.current() == ' ') {
            s.pos++;
        }
        parseCompleteExpression(s, violations);
    }

    // ------------------------------------------------------------------
    // Grammar rule: scalefactor
    //   scalefactor : LIT10 power numeric_power
    //               | LIT10
    //               | LIT1
    //               | VOUFLOAT
    // ------------------------------------------------------------------

    private void parseScaleFactor(ParseState s) {
        if (!s.done() && !Character.isDigit(s.current())) {
            return; // not scale factor
        }

        // LIT1 = "1"
        // LIT10 = "10" (may be followed by '**' → LIT10 power numeric_power)
        // VOUFLOAT = floating point number

        int start = s.pos;

        // Consume leading digits (LIT1 or LIT10, or leading part of VOUFLOAT)
        while (!s.done() && Character.isDigit(s.current())) {
            s.pos++;
        }

        String intPart = s.input.substring(start, s.pos);

        // Check for LIT10 ** numeric_power (e.g. "10**3")
        if ("10".equals(intPart) && !s.done() && s.startsWith("**")) {
            s.pos += 2; // consume '**'
            parseNumericPower(s);
            return;
        }

        // Decimal point
        boolean hasDecimal = !s.done() && s.current() == '.'; // 10.23
        if (hasDecimal) {
            int dp = s.pos++; // consume '.'
            while (!s.done() && Character.isDigit(s.current())) {
                s.pos++;
            }
            if (dp == s.pos) {
                throw new IllegalArgumentException("Expected digits after decimal point at position " + s.pos);
            }
        }

        // Exponent: 'e'/'E'
        boolean hasExponent = false;
        if (!s.done()) {
            if (s.current() == 'e' || s.current() == 'E') { // 10.23E-1
                hasExponent = true;
                s.pos++;
                if (!s.done() && (s.current() == '+' || s.current() == '-')) {
                    s.pos++;
                }
                int expStart = s.pos;
                while (!s.done() && Character.isDigit(s.current())) {
                    s.pos++;
                }
                if (s.pos == expStart) {
                    throw new IllegalArgumentException(
                            "Expected digits after exponent marker at position " + s.pos);
                }
            }
        }

        // If there was no decimal point and no exponent, the token is a plain integer. Only LIT1 ("1") and LIT10 ("10") are allowed as plain integers.
        if (!hasDecimal && !hasExponent) {
            if (!"1".equals(intPart) && !"10".equals(intPart)) {
                if (intPart.charAt(0) == '0' && intPart.length() > 1) {
                    throw new IllegalArgumentException(
                            "Invalid scale factor '" + intPart + "': leading zeros are not allowed.");
                }
            }
        }

        // VOUFLOAT pattern 1: must start with "0" — requires decimal point
        if (s.startsWith("0")) {
            if (!s.startsWith("0.")) {
                throw new IllegalArgumentException(
                        "Invalid VOUFLOAT: '0' must be followed by '.' and digits.");
            }
        }

    }

    // ------------------------------------------------------------------
    // Grammar rule: term
    //   term : unit
    //        | unit power numeric_power
    //   power : STARSTAR  ('**')
    // ------------------------------------------------------------------

    private void parseTerm(ParseState s, List<Violation> violations) {
        parseUnit(s, violations);

        // Optional: power numeric_power (STARSTAR only per grammar)
        if (!s.done() && s.startsWith("**")) {
            s.pos += 2; // consume '**'
            parseNumericPower(s);
        }
    }

    // ------------------------------------------------------------------
    // Grammar rule: unit
    //   unit : STRING
    //        | QUOTED_STRING
    //        | STRING QUOTED_STRING
    //        | PERCENT
    // ------------------------------------------------------------------

    private void parseUnit(ParseState s, List<Violation> violations) {
        if (s.done()) {
            throw new IllegalArgumentException("Unexpected end of input: expected a unit.");
        }

        // PERCENT
        if (s.current() == '%') {
            s.pos++;
            return;
        }

        // QUOTED_STRING  (e.g. 'myunit')
        if (s.current() == '\'') {
            int qp = s.pos;
            parseQuotedString(s);
            violations.add(new Violation(config.severityFor(ViolationType.VOUNIT_QUOTED_IDENTIFIER), ViolationType.VOUNIT_QUOTED_IDENTIFIER,
                    "Quoted unit string found : " + s.input.substring(qp + 1, s.pos - 1)));
            return;
        }

        // STRING (possibly followed by an optional QUOTED_STRING)
        Token token = matchString(s);
        if (token == null) {
            throw new IllegalArgumentException("Broken VOUnit - Expected a unit symbol at position " + s.pos);
        }
        if (token.type.equals(TokenType.UNIT)) {
            String actualInput = s.input.substring(s.pos, s.pos + token.value.length());
            if (!actualInput.equals(token.value)) {
                violations.add(new Violation(config.severityFor(ViolationType.VOUNIT_CASE_SENSITIVE), ViolationType.VOUNIT_CASE_SENSITIVE,
                        "Unit symbol casing mismatch: found '" + actualInput
                                + "' but expected canonical form '" + token.value + "'."));
            }
        } else {
            violations.add(new Violation(config.severityFor(ViolationType.VOUNIT_UNKNOWN_UNIT), ViolationType.VOUNIT_UNKNOWN_UNIT,
                    "Unit symbol expected. Found : '" + token.value + "' at position " + s.pos));
        }
        s.pos += token.value.length();

        // Optional trailing QUOTED_STRING (STRING QUOTED_STRING form)
        if (!s.done() && s.current() == '\'') {
            int qp = s.pos;
            parseQuotedString(s);
            violations.add(new Violation(config.severityFor(ViolationType.VOUNIT_QUOTED_IDENTIFIER), ViolationType.VOUNIT_QUOTED_IDENTIFIER,
                    "Quoted unit string found : " + s.input.substring(qp + 1, s.pos + 1)));
        }
    }

    // ------------------------------------------------------------------
    // Grammar rule: numeric_power
    //   numeric_power : integer
    //                 | parenthesized_number
    //
    //   parenthesized_number : OPEN_P integer CLOSE_P
    //                        | OPEN_P FLOAT CLOSE_P
    //                        | OPEN_P integer division UNSIGNED_INTEGER CLOSE_P
    // ------------------------------------------------------------------

    private void parseNumericPower(ParseState s) {
        if (s.done()) {
            throw new IllegalArgumentException("Expected numeric power at position " + s.pos);
        }

        if (s.current() == '(') {
            // parenthesized_number
            s.pos++; // consume '('

            // Parse integer or float
            if (s.current() == '+' || s.current() == '-') {
                s.pos++;
            }
            int numStart = s.pos;
            while (!s.done() && Character.isDigit(s.current())) {
                s.pos++;
            }
            if (s.pos == numStart) {
                throw new IllegalArgumentException("Expected number inside parenthesized power at position " + s.pos);
            }

            // Optional decimal part → makes it a FLOAT
            if (!s.done() && s.current() == '.') {
                s.pos++;
                while (!s.done() && Character.isDigit(s.current())) {
                    s.pos++;
                }
            } else if (!s.done() && s.current() == '/') {
                // integer division UNSIGNED_INTEGER: e.g. (1/2)
                s.pos++; // consume '/'

                if (s.current() == '+' || s.current() == '-') {
                    throw new IllegalArgumentException("Unsigned integer not allowed in denominator in the numeric power at position " + s.pos);
                }

                int denomStart = s.pos;
                while (!s.done() && Character.isDigit(s.current())) {
                    s.pos++;
                }
                if (s.pos == denomStart) {
                    throw new IllegalArgumentException(
                            "Expected denominator in fractional power at position " + s.pos + " and Found : " + s.current());
                }
            }

            if (s.done() || s.current() != ')') {
                throw new IllegalArgumentException("Expected ')' to close parenthesized power at position " + s.pos);
            }
            s.pos++; // consume ')'
        } else {
            // plain integer
            parseSignedInteger(s);
        }
    }

    private void parseSignedInteger(ParseState s) {
        if (s.done()) {
            throw new IllegalArgumentException("Expected integer at position " + s.pos);
        }
        if (s.current() == '+' || s.current() == '-') {
            s.pos++;
        }
        int start = s.pos;
        while (!s.done() && Character.isDigit(s.current())) {
            s.pos++;
        }
        if (s.pos == start) {
            throw new IllegalArgumentException("Expected digit(s) at position " + s.pos);
        }
    }

    // ------------------------------------------------------------------
    // Quoted string helper
    // ------------------------------------------------------------------

    private void parseQuotedString(ParseState s) {
        s.pos++; // consume opening '\''
        int qp = s.pos;
        while (!s.done() && s.current() != '\'') {
            s.pos++;
        }
        if (qp == s.pos) {
            throw new IllegalArgumentException("Quoted string must contain at least one character.");
        }
        if (s.done()) {
            throw new IllegalArgumentException("Unterminated quoted unit string.");
        }
        s.pos++; // consume closing '\''
    }

    // ------------------------------------------------------------------
    // Symbol-matching helpers
    // ------------------------------------------------------------------

    /**
     * Returns a STRING token (i.e., a valid unit symbol, with or without a prefix)
     * starting at the current position, without advancing {@code s.pos}.
     * Returns {@code null} if no valid symbol starts here.
     */
    private Token matchString(ParseState s) {
        if (s.done() || !Character.isLetter(s.current())) {
            return null;
        }
        String remaining = remainingInput(s);

        for (String function : functions) {
            if (remaining.startsWith(function)) {
                return new Token(TokenType.FUNC, function);
            }
        }

        // Longest-match over known units (direct, then with SI & BI prefix)
        String match = matchUnitSymbolIn(remaining);
        if (match != null) {
            return new Token(TokenType.UNIT, match);
        }

        // Unknown/custom unit: any run of letters/digits (VOUnits allows these)
        int end = 0;
        while (end < remaining.length()
                && (Character.isLetterOrDigit(remaining.charAt(end)))) {
            end++;
        }
        String unknownString = remaining.substring(0, end);
        return end > 0 ? new Token(TokenType.UNKNOWN, unknownString) : null;
    }

    /**
     * Tries to match a known (or prefixed) unit symbol in {@code remaining}.
     * Returns the matched token, or {@code null}.
     */
    private String matchUnitSymbolIn(String remaining) {
        // 1. Direct match — try longest symbols first to avoid "m" shadowing "min"
        String bestDirect = getLongestMatchingUnit(remaining);
        if (bestDirect != null) {
            return bestDirect;
        }

        // 2. SI prefix and known unit
        for (Prefix prefixObj : Prefix.values()) {
            String prefix = prefixObj.getSymbol();

            if (!remaining.startsWith(prefix)) {
                continue;
            }

            boolean isBinary = prefixObj.getType().equals(PrefixType.BI);
            String rest = remaining.substring(prefix.length());
            bestDirect = getLongestMatchingUnit(rest);

            if (bestDirect == null) {
                if (isBinary && rest.charAt(0) == '\'') {
                    throw new IllegalArgumentException("Binary prefix '" + prefix + "' cannot be used with an unknown quoted unit: " + rest);
                }
                continue;
            }

            Optional<VOUnit> unit = VOUnit.fromSymbol(bestDirect);
            if (unit.isEmpty()) {
                throw new IllegalArgumentException("BUG: unit not found for " + bestDirect + " (prefix: " + prefix + ")");
            }
            if (unit.get().isAllowedPrefix(prefixObj.getType())) {
                return prefix + bestDirect;
            }
        }

        return null;
    }

    private String getLongestMatchingUnit(String remaining) {
        String longestMatch = null;
        for (String unit : VOUnit.getAllSymbols()) {
            boolean matchFound = remaining.regionMatches(true, 0, unit, 0, unit.length());
            if (matchFound) {
                String after = remaining.substring(unit.length());
                if (isUnitTerminator(after)) {
                    if (longestMatch == null || unit.length() > longestMatch.length()) {
                        longestMatch = unit;
                    }
                }
            }
        }

        return longestMatch;
    }

    /**
     * Returns {@code true} if {@code after} is a valid character sequence that
     * may follow a unit symbol (operator, exponent marker, closing paren, or end).
     */
    private boolean isUnitTerminator(String after) {
        if (after.isEmpty()) {
            return true;
        }
        char c = after.charAt(0);
        return c == '.' || c == '/' || c == ')' || c == '*' || c == ' '
                || c == '-' || c == '+' || Character.isDigit(c);
    }

    private String remainingInput(ParseState s) {
        return s.input.substring(s.pos);
    }

    // ------------------------------------------------------------------
    // ParseState - Lexer
    // ------------------------------------------------------------------

    private static class ParseState {
        final String input;
        int pos = 0;

        ParseState(String input) {
            this.input = input;
        }

        boolean done() {
            return pos >= input.length();
        }

        char current() {
            return input.charAt(pos);
        }

        boolean startsWith(String prefix) {
            return input.startsWith(prefix, pos);
        }
    }

    public enum TokenType {
        UNIT, FUNC, UNKNOWN
    }

    public static class Token {
        final TokenType type;
        final String value;

        public Token(TokenType type, String value) {
            this.type = type;
            this.value = value;
        }

        public TokenType getType() {
            return type;
        }

        public String getValue() {
            return value;
        }
    }
}
