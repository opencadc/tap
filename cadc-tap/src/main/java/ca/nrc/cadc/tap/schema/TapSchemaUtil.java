/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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

package ca.nrc.cadc.tap.schema;

import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class TapSchemaUtil {

    private static final Logger log = Logger.getLogger(TapSchemaUtil.class);

    private TapSchemaUtil() {
    }

    /**
     * Create a tap_schema description of a VOTable.
     *
     * @param schemaName
     * @param tableName
     * @param votable
     * @return
     */
    public static TableDesc createTableDesc(String schemaName, String tableName, VOTableTable votable) {
        try {
            checkValidIdentifier(schemaName);
        } catch (ADQLIdentifierException ex) {
            throw new IllegalArgumentException("invalid ADQL identifier (schema name): " + schemaName, ex);
        }
        try {
            checkValidTableName(tableName);
        } catch (ADQLIdentifierException ex) {
            throw new IllegalArgumentException("invalid ADQL identifier (table name): " + tableName, ex);
        }
        
        TableDesc ret = new TableDesc(schemaName, tableName);
        if (votable == null) {
            throw new IllegalArgumentException("invalid input: no VOTable with column metadata");
        }

        for (VOTableField f : votable.getFields()) {
            try {
                checkValidIdentifier(f.getName());
                ColumnDesc columnDesc = TapSchemaUtil.convert(tableName, f);
                log.debug("column: " + f + " -> " + columnDesc);
                ret.getColumnDescs().add(columnDesc);
            } catch (ADQLIdentifierException ex) {
                throw new IllegalArgumentException("invalid ADQL identifier (column name): " + f.getName(), ex);
            }
        }
        return ret;
    }
    
    /**
     * Create a VOTableField from a ColumnDesc.
     *
     * @param column
     * @return The associated VOTableField
     */
    public static VOTableField convert(ColumnDesc column) {
        VOTableField vtf = new VOTableField(
            column.getColumnName(), column.getDatatype().getDatatype(),
            column.getDatatype().arraysize);
        vtf.xtype = column.getDatatype().xtype;
        return vtf;
    }

    /**
     * Convert a VOTable field into tap_schema column descriptor.
     *
     * @param tableName
     * @param field
     * @return
     */
    public static ColumnDesc convert(String tableName, VOTableField field) {
        TapDataType dt = new TapDataType(field.getDatatype(), field.getArraysize(), field.xtype);
        ColumnDesc ret = new ColumnDesc(tableName, field.getName(), dt);
        ret.description = field.description;
        ret.columnID = field.id;
        ret.ucd = field.ucd;
        ret.unit = field.unit;
        ret.utype = field.utype;

        ret.indexed = false;
        ret.principal = false;
        ret.std = false;

        return ret;
    }
    
    public static void checkValidTableName(String identifier)  throws ADQLIdentifierException {
        String[] parts = identifier.split("[.]");
        String schemaName = null;
        String tableName = identifier;
        if (parts.length == 2) {
            schemaName = parts[0];
            tableName = parts[1];
        } else if (parts.length > 2) {
            throw new ADQLIdentifierException("invalid table name: " + identifier + " (too many parts)");
        }

        if (schemaName != null) {
            checkValidIdentifier(schemaName);
        }
        
        checkValidIdentifier(tableName);
    }

    /**
     * Validates that the given String is a valid ADQL identifier.
     *
     * @param identifier String to be tested.
     * @throws ADQLIdentifierException
     */
    public static void checkValidIdentifier(String identifier) throws ADQLIdentifierException {
        // identifier shouldn't be null and cannot be an empty string.
        if (identifier == null || identifier.trim().isEmpty()) {
            throw new ADQLIdentifierException("Identifier is null or empty");
        }

        // identifier cannot start with, contain, or end, with a space.
        if (identifier.startsWith(" ") || identifier.contains(" ") || identifier.endsWith(" ")) {
            throw new ADQLIdentifierException("Identifier contains spaces");
        }

        // identifier must start with a letter {aA-zZ}.
        if (!isAsciiLetter(identifier.charAt(0))) {
            throw new ADQLIdentifierException("Identifier must start with a letter");
        }

        // subsequent characters must be letters, underscores, or digits.
        for (int i = 1; i < identifier.length(); i++) {
            if (!isValidIdentifierCharacter(identifier.charAt(i))) {
                throw new ADQLIdentifierException("Identifier contains an invalid character " + identifier.charAt(i));
            }
        }
    }

    /**
     * Checks if the char is a valid US ASCII letter.
     *
     * @param c char to test.
     * @return true if the char is valid ASCII, false otherwise.
     */
    public static boolean isAsciiLetter(char c) {
        if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
            return true;
        }
        return false;
    }

    /**
     * Checks if the char is either a US ASCII char, an underscore,
     * of a digit.
     *
     * @param c char to test.
     * @return true if the char is a valid ADQL identifier char, false otherwise.
     */
    public static boolean isValidIdentifierCharacter(char c) {
        if (c == '_' || isAsciiLetter(c) || Character.isDigit(c)) {
            return true;
        }
        return false;
    }
}
