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

package ca.nrc.cadc.tap.schema.validator.ucd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * Validates UCD1+ strings against the IVOA rules as defined in "UCD - An IVOA Standard for Unified Content Descriptors" v1.1
 * And "UCD1+ controlled vocabulary" v1.6
 */
public final class UCDValidator {

    private static final Logger log = Logger.getLogger(UCDValidator.class);

    /**
     * Regex for a single UCD1+ word/atom token.
     * Allows: letters, digits, dots (hierarchy), hyphens (e.g. em.X-ray, em.IR.3-4um).
     */
    private static final Pattern WORD_PATTERN =
            Pattern.compile("[A-Za-z][A-Za-z0-9]*(\\.[A-Za-z0-9][A-Za-z0-9\\-]*)*");

    public UCDValidator() {
    }

    /**
     * Validates a UCD1+ string and returns a detailed result.
     *
     * @param ucd the UCD string to validate
     * @return a {@link UCDValidationResult} describing validity and any violations
     */
    public UCDValidationResult validate(String ucd) {
        log.debug("Validating UCD string: " + ucd);
        List<UCDValidationResult.Violation> violations = new ArrayList<>();

        if (ucd == null || ucd.isBlank()) {
            violations.add(error("UCD must not be null or blank"));
            return new UCDValidationResult(ucd, violations);
        }

        if (ucd.startsWith(";") || ucd.endsWith(";")) {
            violations.add(error("UCD must not start or end with a semicolon"));
        }

        // split into individual words
        String[] words = ucd.split(";", -1);
        if (words.length == 0) {
            violations.add(error("UCD contains no words"));
            return new UCDValidationResult(ucd, violations);
        }

        for (int i = 0; i < words.length; i++) {
            String word = words[i];

            if (!WORD_PATTERN.matcher(word).matches()) {
                violations.add(error(
                        "Word \"" + word + "\" at position " + (i + 1)
                                + " does not match the allowed UCD token pattern "
                                + "[A-Za-z][A-Za-z0-9]*(\\.[A-Za-z0-9][A-Za-z0-9-]*)* "));
                continue;
            }

            Optional<UCDWord> vocabEntry = UCDVocabulary.lookup(word);
            boolean inVocab = vocabEntry.isPresent();

            if (inVocab && !vocabEntry.get().getWord().equals(word)) {
                // word found case-insensitively but does not match the official casing
                violations.add(warning(
                        "Word \"" + word + "\" at position " + (i + 1)
                                + " does not match the official capitalisation \""
                                + vocabEntry.get().getWord() + "\""));
            }

            if (!inVocab) {
                violations.add(error("Word \"" + word + "\" at position " + (i + 1)
                        + " is not in the IVOA UCD1+ controlled vocabulary"));
                continue;
            }

            UCDWord ucdWord = vocabEntry.get();
            boolean isPrimary = (i == 0);

            // primary word must not be S-only
            if (isPrimary && !ucdWord.canBePrimary()) {
                violations.add(error(
                        "Word \"" + word + "\" has syntax flag "
                                + ucdWord.getFlag().name()
                                + " (secondary-only) and cannot be used as the primary word"));
            }

            // secondary word must not be P-only
            if (!isPrimary && !ucdWord.canBeSecondary()) {
                violations.add(error(
                        "Word \"" + word + "\" at position " + (i + 1)
                                + " has syntax flag " + ucdWord.getFlag().name()
                                + " (primary-only) and cannot be used as a secondary word"));
            }
        }
        return new UCDValidationResult(ucd, violations);
    }

    private static UCDValidationResult.Violation error(String message) {
        return new UCDValidationResult.Violation(UCDValidationResult.Violation.Severity.ERROR, message);
    }

    private static UCDValidationResult.Violation warning(String message) {
        return new UCDValidationResult.Violation(UCDValidationResult.Violation.Severity.WARNING, message);
    }

    public static final class UCDValidationResult {

        public static final class Violation {

            public enum Severity { ERROR, WARNING }

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

        private final String input;
        private final boolean valid;
        private final List<Violation> violations;

        public UCDValidationResult(String input, List<Violation> violations) {
            this.input = input;
            this.violations = Collections.unmodifiableList(violations);
            this.valid = violations.stream()
                    .noneMatch(v -> v.getSeverity() == Violation.Severity.ERROR);
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

        @Override
        public String toString() {
            if (valid) {
                return "VALID: \"" + input + "\"";
            }
            StringBuilder sb = new StringBuilder("INVALID: \"").append(input).append("\"\n");
            violations.forEach(v -> sb.append("  ").append(v).append("\n"));
            return sb.toString().stripTrailing();
        }
    }

}

