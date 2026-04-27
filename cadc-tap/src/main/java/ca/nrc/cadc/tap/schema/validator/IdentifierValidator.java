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

package ca.nrc.cadc.tap.schema.validator;

import ca.nrc.cadc.tap.schema.TapSchemaUtil;
import ca.nrc.cadc.tap.schema.validator.adql.ReservedKeyword;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates Identifiers (Schema name, Table name, Column name) strings.
 */
public class IdentifierValidator {

    public enum IdentifierType {
        SCHEMA_NAME, TABLE_NAME, COLUMN_NAME
    }

    private final ValidatorConfig config;

    public IdentifierValidator() {
        this(ValidatorConfig.lax());
    }

    public IdentifierValidator(ValidatorConfig config) {
        this.config = config;
    }

    /**
     * Validates that the given String is a valid ADQL identifier.
     *
     * @param identifier String to be tested.
     */
    public ValidationResult checkValidIdentifier(String identifier, IdentifierType identifierType) {

        List<Violation> violations = new ArrayList<>();

        // identifier shouldn't be null and cannot be an empty string.
        if (identifier == null || identifier.trim().isEmpty()) {
            violations.add(new Violation(ViolationType.NULL_OR_BLANK, "Identifier is null or empty"));
            return new ValidationResult(identifier, violations, config);
        }

        // Identifier cannot be a reserved keyword.
        if (ReservedKeyword.isReserved(identifier)) {
            violations.add(new Violation(ViolationType.IDENTIFIER_RESERVED_KEYWORD, "Identifier '" + identifier + "' is a reserved keyword."));
        }

        // identifier cannot start with, contain, or end, with a space.
        if (identifier.startsWith(" ") || identifier.contains(" ") || identifier.endsWith(" ")) {
            violations.add(new Violation(ViolationType.STRUCTURAL, "Identifier contains spaces"));
            identifier = identifier.replaceAll(" ", "");
        }

        if (identifierType.equals(IdentifierType.COLUMN_NAME) && identifier.startsWith("\"") && identifier.endsWith("\"")) {
            violations.add(new Violation(ViolationType.IDENTIFIER_QUOTED,
                    "Identifier is double quoted. Defining identifiers using double quotes is discouraged."));
            identifier = identifier.substring(1, identifier.length() - 1);
        }

        // identifier must start with a letter {aA-zZ}.
        if (!TapSchemaUtil.isAsciiLetter(identifier.charAt(0))) {
            violations.add(new Violation(ViolationType.STRUCTURAL, "Identifier must start with a letter"));
        }

        // subsequent characters must be letters, underscores, or digits.
        for (int i = 1; i < identifier.length(); i++) {
            if (!TapSchemaUtil.isValidIdentifierCharacter(identifier.charAt(i))) {
                violations.add(new Violation(ViolationType.IDENTIFIER_INVALID_CHAR, "Identifier contains an invalid character '" + identifier.charAt(i) + "'"));
            }
        }

        return new ValidationResult(identifier, violations, config);
    }

    public ValidationResult checkValidTableName(String identifier) {
        List<Violation> violations = new ArrayList<>();
        String[] parts = identifier.split("[.]");
        String schemaName = null;
        String tableName = identifier;
        if (parts.length == 2) {
            schemaName = parts[0];
            tableName = parts[1];
        } else if (parts.length > 2) {
            violations.add(new Violation(ViolationType.STRUCTURAL, "invalid table name: " + identifier + " (too many parts)"));
            return new ValidationResult(identifier, violations, config);
        }

        if (schemaName != null) {
            violations.addAll(checkValidIdentifier(schemaName, IdentifierType.SCHEMA_NAME).getViolations());
        }

        violations.addAll(checkValidIdentifier(tableName, IdentifierType.TABLE_NAME).getViolations());
        return new ValidationResult(identifier, violations, config);
    }

}
