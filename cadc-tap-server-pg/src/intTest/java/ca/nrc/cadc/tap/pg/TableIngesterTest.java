/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2024.                            (c) 2024.
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
 *  : 5 $
 *
 ************************************************************************
 */

package ca.nrc.cadc.tap.pg;

import ca.nrc.cadc.tap.db.TableCreator;
import ca.nrc.cadc.tap.db.TableIngester;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import ca.nrc.cadc.util.Log4jInit;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class TableIngesterTest extends TestUtil {
    private static final Logger log = Logger.getLogger(TableIngesterTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.db", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.tap.schema", Level.INFO);
    }

    private final TapSchemaDAO tapSchemaDAO;

    public TableIngesterTest() {
        super();
        this.tapSchemaDAO = new TapSchemaDAO();
        this.tapSchemaDAO.setDataSource(this.dataSource);
    }

    @Test
    public void testTableIngest() {
        String testTable = testSchemaName + ".testTableIngest";
        TableCreator tableCreator = new TableCreator(dataSource);
        try {
            // cleanup
            try {
                tableCreator.dropTable(testTable);
            } catch (Exception ignore) {
                log.debug("database-cleanup-before-test failed for " + testTable);
            }
            try {
                tapSchemaDAO.delete(testTable);
            } catch (Exception ignore) {
                log.debug("tap_schema-cleanup-before-test failed for " + testTable);
            }

            // create test table in the database
            TableDesc ingestTable = getTableDesc(testSchemaName, testTable);
            tableCreator.createTable(ingestTable);
            log.info("created database table: " + testTable);

            // ingest table into the tap_schema
            TableIngester tableIngester = new TableIngester(dataSource);
            tableIngester.ingest(testSchemaName, testTable);
            log.info("ingested table");

            // compare database and tap_schema
            SchemaDesc schemaDesc = tapSchemaDAO.getSchema(testSchemaName, true);
            Assert.assertNotNull("schema", schemaDesc);

            TableDesc tableDesc = tapSchemaDAO.getTable(testTable);
            Assert.assertNotNull("table", tableDesc);

            List<ColumnDesc> databaseColumns = ingestTable.getColumnDescs();
            List<ColumnDesc> tapSchemaColumns = tableDesc.getColumnDescs();
            for (ColumnDesc databaseColumn: databaseColumns) {
                boolean found = false;
                log.info("database column: " + databaseColumn.getColumnName());
                for (ColumnDesc tapSchemaColumn : tapSchemaColumns) {
                    log.debug("tap_schema column: " + tapSchemaColumn.getColumnName());
                    if (databaseColumn.getColumnName().equals(tapSchemaColumn.getColumnName())) {
                        log.info("compare: " + tapSchemaColumn.getDatatype() + " vs " + databaseColumn.getDatatype());
                        // NOTE: TapDataType.equals() is lax when comparing fixed size and variable size so while
                        // the TableIngester DOES NOT assign column sizes to arrays it goes unnoticed here even
                        // though the test table has explicit sizes specified on input
                        Assert.assertEquals("datatype", databaseColumn.getDatatype(), tapSchemaColumn.getDatatype());
                        found = true;
                        break;
                    }
                }
                Assert.assertTrue("tap_schema column not found: " + databaseColumn, found);
            }

            // cleanup
            try {
                tableCreator.dropTable(testTable);
                log.debug("dropped table: " + testTable);
            } catch (Exception ignore) {
                log.debug("database-cleanup-after-test failed for " + testTable);
            }
            try {
                tapSchemaDAO.delete(testTable);
            } catch (Exception ignore) {
                log.debug("tap_schema-cleanup-after-test failed for " + testTable);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testVerifyTableMetadata() {
        String testTableName = "testVerifyTableMetadata";
        String testTable = testSchemaName + "." + testTableName;

        try {
            // cleanup
            TableCreator tableCreator = new TableCreator(dataSource);
            try {
                tableCreator.dropTable(testTable);
            } catch (Exception ignore) {
                log.debug("database-cleanup-before-test failed for " + testTable);
            }

            // create test table in the database
            TableDesc ingestTable = getTableDesc(testSchemaName, testTable);
            tableCreator.createTable(ingestTable);
            log.debug("created database table: " + testTable);

            try (Connection connection = dataSource.getConnection()) {
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet rs = metaData.getColumns(null, testSchemaName, testTableName.toLowerCase(), null)) {
                    while (rs.next()) {
                        String colName = rs.getString("COLUMN_NAME");
                        String ctype = rs.getString("TYPE_NAME");
                        //int clen = rs.getInt("COLUMN_SIZE");
                        
                        switch (colName) {
                            case "e7":
                                Assert.assertEquals("interval", "polygon", ctype);
                                break;
                            case "e8":
                                Assert.assertEquals("point", "spoint", ctype);
                                break;
                            case "e9":
                                Assert.assertEquals("circle", "scircle", ctype);
                                break;
                            case "e10":
                                Assert.assertEquals("polygon", "spoly", ctype);
                                break;
                            case "a11":
                                Assert.assertEquals("short[]", "_int2", ctype);
                                //Assert.assertEquals(8, clen);
                                break;
                            case "a12":
                                Assert.assertEquals("int[]", "_int4", ctype);
                                //Assert.assertEquals(16, clen);
                                break;
                            case "a13":
                                Assert.assertEquals("long[]", "_int8", ctype);
                                //Assert.assertEquals(64, clen);
                                break;
                            case "a14":
                                Assert.assertEquals("float[]", "_float4", ctype);
                                break;
                            case "a15":
                                Assert.assertEquals("double[]", "_float8", ctype);
                                //Assert.assertEquals(2, clen);
                                break;
                            default:
                                Assert.fail("unexpected column: " + colName);
                        }
                    }
                }
            }

            // cleanup
            try {
                tableCreator.dropTable(testTable);
                log.debug("dropped table: " + testTable);
            } catch (Exception ignore) {
                log.debug("database-cleanup-after-test failed for " + testTable);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPrintTableMetadata() {
        String testTableName = "testPrintTableMetadata";
        String testTable = testSchemaName + "." + testTableName;
        try {
            // cleanup
            TableCreator tableCreator = new TableCreator(dataSource);
            try {
                tableCreator.dropTable(testTable);
            } catch (Exception ignore) {
                log.info("database-cleanup-before-test failed for " + testTable);
            }

            // create test table in the database
            TableDesc ingestTable = getTableDesc(testSchemaName, testTable);
            tableCreator.createTable(ingestTable);
            log.info("created database table: " + testTable);

            List<String> columnInfo = new ArrayList<String>() {{
                add("DATA_TYPE");
                add("TYPE_NAME");
                add("COLUMN_SIZE");
                add("COLUMN_DEF");
                add("DECIMAL_DIGITS");
                add("SQL_DATA_TYPE");
                add("SQL_DATETIME_SUB");
                add("CHAR_OCTET_LENGTH");
                add("SOURCE_DATA_TYPE");
                add("REMARKS");
                add("IS_NULLABLE");
            }};

            List<String> indexInfo = new ArrayList<String>() {{
                add("NON_UNIQUE");
                add("INDEX_QUALIFIER");
                add("INDEX_NAME");
                add("TYPE");
                add("ASC_OR_DESC");
            }};

            try (Connection connection = dataSource.getConnection()) {
                // create an index on the table
                JdbcTemplate jdbc = new JdbcTemplate(dataSource);
                String index = "CREATE UNIQUE INDEX a11_idx ON " + testSchemaName + ".testPrintTableMetadata (a11)";
                log.info("sql:\n" + index);
                jdbc.execute(index);

                log.info("column metadata");
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet rs = metaData.getColumns(null, testSchemaName, testTableName.toLowerCase(), null)) {
                    while (rs.next()) {
                        String colName = rs.getString("COLUMN_NAME");
                        log.info(String.format("columnName: %s", colName));
                        for (String info : columnInfo) {
                            log.info(String.format("%s.%s: %s", colName, info, rs.getString(info)));
                        }
                    }
                }

                log.info("index metadata");
                try (ResultSet rs = metaData.getIndexInfo(null, testSchemaName, testTableName.toLowerCase(), false, false)) {
                    while (rs.next()) {
                        String colName = rs.getString("COLUMN_NAME");
                        log.info(String.format("columnName: %s", colName));
                        for (String info : indexInfo) {
                            log.info(String.format("%s.%s: %s", colName, info, rs.getString(info)));
                        }
                    }
                }

                // cleanup
                try {
                    tableCreator.dropTable(testTable);
                    log.info("dropped table: " + testTable);
                } catch (Exception ignore) {
                    log.info("database-cleanup-after-test failed for " + testTable);
                }
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    TableDesc getTableDesc(String schemaName, String tableName) throws Exception {
        final TableDesc tableDesc = new TableDesc(schemaName, tableName);
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "e7", TapDataType.INTERVAL));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "e8", TapDataType.POINT));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "e9", TapDataType.CIRCLE));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "e10", TapDataType.POLYGON));
        // arrays
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "a11", new TapDataType("short", "8", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "a12", new TapDataType("int", "16", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "a13", new TapDataType("long", "64*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "a14", new TapDataType("float", "*", null)));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "a15", new TapDataType("double", "2", null)));

        return tableDesc;
    }

}
