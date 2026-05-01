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

package ca.nrc.cadc.tap.schema;

import ca.nrc.cadc.tap.schema.validator.ValidatorConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * Validates the Metadata Validation Configurations - Strict, Lax and None.
 */
public class ValidatorConfigTest {

    @Test
    public void testValidateLaxConfig() {
        ValidatorConfig defaultConfig = ValidatorConfig.lax();

        String schemaName = "schema.name";
        String tableName = "tableName";
        TableDesc table = new TableDesc(schemaName, tableName);
        ColumnDesc col1 = new ColumnDesc(tableName, "col1", new TapDataType("double"));
        col1.unit = "Jy/beam/RMSF"; // invalid - two '/' on the top level is not allowed
        col1.ucd = "phot.flux"; // valid - E flag
        table.getColumnDescs().add(col1);
        ColumnDesc col2 = new ColumnDesc(tableName, "\"select\"", new TapDataType("double"));
        table.getColumnDescs().add(col2);
        col2.ucd = "unknown.ucd"; // invalid - unknown UCD
        col2.unit = "unknown.unit"; // invalid - unknown unit

        String result;
        try {
            result = TapSchemaUtil.validateTableDesc(table, defaultConfig);
            Assert.fail("Expected exception");
        } catch (Exception e) {
            // expected
            result = e.getMessage();
        }

        Assert.assertTrue(result != null && !result.isEmpty());
        Assert.assertTrue(result.contains("errors: 3"));
        Assert.assertTrue(result.contains("warnings: 3"));

        Assert.assertTrue(result.contains("schema.name"));
        Assert.assertFalse(result.contains("tableName"));

        Assert.assertFalse(result.contains("col1"));
        Assert.assertTrue(result.contains("\"select\""));

        Assert.assertTrue(result.contains("Jy/beam/RMSF")); // STRUCTURAL
        Assert.assertFalse(result.contains("phot.flux")); // Valid
        Assert.assertTrue(result.contains("unknown.ucd")); // UCD_UNKNOWN_WORD
        Assert.assertTrue(result.contains("unknown.unit")); //VOUNIT_UNKNOWN_UNIT

        // Fix error issues and keep warnings
        table.setSchemaName("schema_name");
        col1.unit = "Jy/beam";
        col2.ucd = "phot.flux";

        try {
            result = TapSchemaUtil.validateTableDesc(table, defaultConfig);
        } catch (Exception e) {
            Assert.fail("Expected exception");
        }

        Assert.assertTrue(result != null && !result.isEmpty());
        Assert.assertTrue(result.contains("errors: 0")); // Fixed
        Assert.assertTrue(result.contains("warnings: 3")); // did not fix
        Assert.assertFalse(result.contains("schema_name"));
        Assert.assertTrue(result.contains("\"select\""));
        Assert.assertFalse(result.contains("phot.flux"));
    }

    @Test
    public void testValidateStrictConfig() {
        ValidatorConfig defaultConfig = ValidatorConfig.strict();

        String schemaName = "schema.name";
        String tableName = "tableName";
        TableDesc table = new TableDesc(schemaName, tableName);
        ColumnDesc col1 = new ColumnDesc(tableName, "col1", new TapDataType("double"));
        col1.unit = "Jy/beam/RMSF"; // invalid - two '/' on the top level is not allowed
        col1.ucd = "phot.flux"; // valid - E flag
        table.getColumnDescs().add(col1);
        ColumnDesc col2 = new ColumnDesc(tableName, "\"select\"", new TapDataType("double"));
        table.getColumnDescs().add(col2);
        col2.ucd = "unknown.ucd"; // invalid - unknown UCD
        col2.unit = "unknown.unit"; // invalid - unknown unit

        String result;
        try {
            result = TapSchemaUtil.validateTableDesc(table, defaultConfig);
            Assert.fail("Expected exception");
        } catch (Exception e) {
            // expected
            result = e.getMessage();
        }

        Assert.assertTrue(result != null && !result.isEmpty());
        Assert.assertTrue(result.contains("errors: 6"));
        Assert.assertTrue(result.contains("warnings: 0"));

        Assert.assertTrue(result.contains("schema.name"));
        Assert.assertFalse(result.contains("tableName"));

        Assert.assertFalse(result.contains("col1"));
        Assert.assertTrue(result.contains("\"select\""));

        Assert.assertTrue(result.contains("Jy/beam/RMSF")); // STRUCTURAL
        Assert.assertFalse(result.contains("phot.flux"));
        Assert.assertTrue(result.contains("unknown.ucd")); // UCD_UNKNOWN_WORD
        Assert.assertTrue(result.contains("unknown.unit")); //VOUNIT_UNKNOWN_UNIT

        // Fix all the errors
        table.setSchemaName("schema_name");
        col1.unit = "Jy/beam";
        table.getColumnDescs().remove(col2);
        col2 = new ColumnDesc(tableName, "validColumnName", new TapDataType("double"));
        col2.ucd = "phot.flux";
        col2.unit = "G";
        table.getColumnDescs().add(col2);

        try {
            result = TapSchemaUtil.validateTableDesc(table, defaultConfig);
        } catch (Exception e) {
            Assert.fail("Unexpected exception: " + e);
        }
        Assert.assertTrue(result == null || result.isEmpty());
    }

    @Test
    public void testValidateNoneConfig() {
        ValidatorConfig defaultConfig = ValidatorConfig.none();

        String schemaName = "schema-name";
        String tableName = "tableName";
        TableDesc table = new TableDesc(schemaName, tableName);
        ColumnDesc col1 = new ColumnDesc(tableName, "\"select\"", new TapDataType("double"));
        col1.unit = "Jy/beam/RMSF"; // invalid - two '/' on the top level is not allowed
        col1.ucd = "phot.flux"; // valid - E flag
        table.getColumnDescs().add(col1);
        ColumnDesc col2 = new ColumnDesc(tableName, "col2", new TapDataType("double"));
        table.getColumnDescs().add(col2);

        String result;
        try {
            result = TapSchemaUtil.validateTableDesc(table, defaultConfig);
            Assert.fail("Expected exception");
        } catch (Exception e) {
            result = e.getMessage();
        }
        Assert.assertTrue(result != null && !result.isEmpty());
        Assert.assertTrue(result.contains("errors: 1"));
        Assert.assertTrue(result.contains("warnings: 0"));
        Assert.assertTrue(result.contains("schema-name"));
        Assert.assertFalse(result.contains("tableName"));

        Assert.assertFalse(result.contains("col1"));
        Assert.assertFalse(result.contains("\"select\"")); // okay for "none" config

        Assert.assertFalse(result.contains("Jy/beam/RMSF")); // did not get validated for "none" config
        Assert.assertFalse(result.contains("phot.flux"));
    }

}
