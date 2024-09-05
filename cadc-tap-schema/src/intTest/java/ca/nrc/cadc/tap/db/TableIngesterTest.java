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

package ca.nrc.cadc.tap.db;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import ca.nrc.cadc.util.Log4jInit;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class TableIngesterTest {
    private static final Logger log = Logger.getLogger(TableIngesterTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.db", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.tap.schema", Level.DEBUG);
    }

    private final DataSource dataSource;
    private final TapSchemaDAO tapSchemaDAO;
    private final String TEST_SCHEMA = "int_test_schema";

    public TableIngesterTest() {
        // create a datasource and register with JNDI
        try {
            DBConfig conf = new DBConfig();
            ConnectionConfig cc = conf.getConnectionConfig("TAP_SCHEMA_TEST", "cadctest");
            this.dataSource = DBUtil.getDataSource(cc);
            log.info("configured data source: " + cc.getServer() + "," + cc.getDatabase() + "," + cc.getDriver() + "," + cc.getURL());
            this.tapSchemaDAO = new TapSchemaDAO();
            this.tapSchemaDAO.setDataSource(this.dataSource);
        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw new IllegalStateException("failed to create DataSource", ex);
        }
    }

    @Test
    public void testTableIngest() {
        String testTable = TEST_SCHEMA + ".testTableIngest";
        try {
            // cleanup
            TableCreator tableCreator = new TableCreator(dataSource);
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
            TableDesc ingestTable = getTableDesc(TEST_SCHEMA, testTable);
            tableCreator.createTable(ingestTable);
            log.debug("created database table: " + testTable);

            // ingest table into the tap_schema
            TableIngester tableIngester = new TableIngester(dataSource);
            tableIngester.ingest(TEST_SCHEMA, testTable);
            log.debug("ingested table");

            // check

            // compare database and tap_schema
            SchemaDesc schemaDesc = tapSchemaDAO.getSchema(TEST_SCHEMA, true);
            Assert.assertNotNull("schema", schemaDesc);

            TableDesc tableDesc = tapSchemaDAO.getTable(testTable);
            Assert.assertNotNull("table", tableDesc);

            List<ColumnDesc> databaseColumns = ingestTable.getColumnDescs();
            List<ColumnDesc> tapSchemaColumns = tableDesc.getColumnDescs();
            for (ColumnDesc databaseColumn: databaseColumns) {
                boolean found = false;
                log.debug("database column: " + databaseColumn.getColumnName());
                for (ColumnDesc tapSchemaColumn : tapSchemaColumns) {
                    log.debug("tap_schema column: " + tapSchemaColumn.getColumnName());
                    if (databaseColumn.getColumnName().equals(tapSchemaColumn.getColumnName())) {
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
        String testTable = TEST_SCHEMA + "." + testTableName;

        try {
            // cleanup
            TableCreator tableCreator = new TableCreator(dataSource);
            try {
                tableCreator.dropTable(testTable);
            } catch (Exception ignore) {
                log.debug("database-cleanup-before-test failed for " + testTable);
            }

            // create test table in the database
            TableDesc ingestTable = getTableDesc(TEST_SCHEMA, testTable);
            tableCreator.createTable(ingestTable);
            log.debug("created database table: " + testTable);

            try (Connection connection = dataSource.getConnection()) {
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet rs = metaData.getColumns(null, TEST_SCHEMA, testTableName.toLowerCase(), null)) {
                    while (rs.next()) {
                        String colName = rs.getString("COLUMN_NAME");
                        switch (colName) {
                            case "c0":
                                Assert.assertEquals("String", "varchar", rs.getString("TYPE_NAME"));
                                break;
                            case "c1":
                                Assert.assertEquals("short", "int2", rs.getString("TYPE_NAME"));
                                break;
                            case "c2":
                                Assert.assertEquals("integer", "int4", rs.getString("TYPE_NAME"));
                                break;
                            case "c3":
                                Assert.assertEquals("long", "int8", rs.getString("TYPE_NAME"));
                                break;
                            case "c4":
                                Assert.assertEquals("float", "float4", rs.getString("TYPE_NAME"));
                                break;
                            case "c5":
                                Assert.assertEquals("double", "float8", rs.getString("TYPE_NAME"));
                                break;
                            case "c6":
                                Assert.assertEquals("timestamp", "timestamp", rs.getString("TYPE_NAME"));
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

    @Ignore
    @Test
    public void testPrintTableMetadata() {
        String testTableName = "testPrintTableMetadata";
        String testTable = TEST_SCHEMA + "." + testTableName;
        try {
            // cleanup
            TableCreator tableCreator = new TableCreator(dataSource);
            try {
                tableCreator.dropTable(testTable);
            } catch (Exception ignore) {
                log.debug("database-cleanup-before-test failed for " + testTable);
            }

            // create test table in the database
            TableDesc ingestTable = getTableDesc(TEST_SCHEMA, testTable);
            tableCreator.createTable(ingestTable);
            log.debug("created database table: " + testTable);

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
                // create two indexes on the table
                JdbcTemplate jdbc = new JdbcTemplate(dataSource);
                String index1 = "CREATE UNIQUE INDEX c0_idx ON int_test_schema.testPrintTableMetadata (c0)";
                log.debug("sql:\n" + index1);
                jdbc.execute(index1);
                String index2 = "CREATE UNIQUE INDEX c6_idx ON int_test_schema.testPrintTableMetadata (c6)";
                log.debug("sql:\n" + index2);
                jdbc.execute(index2);

                log.info("column metadata");
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet rs = metaData.getColumns(null, TEST_SCHEMA, testTableName.toLowerCase(), null)) {
                    while (rs.next()) {
                        String colName = rs.getString("COLUMN_NAME");
                        log.info(String.format("columnName: %s", colName));
                        for (String info : columnInfo) {
                            log.info(String.format("%s.%s: %s", colName, info, rs.getString(info)));
                        }
                    }
                }

                log.info("index metadata");
                try (ResultSet rs = metaData.getIndexInfo(null, TEST_SCHEMA, testTableName.toLowerCase(), false, false)) {
                    while (rs.next()) {
                        String colName = rs.getString("COLUMN_NAME");
                        log.info(String.format("columnName: %s", colName));
                        for (String info : indexInfo) {
                            log.info(String.format("%s.%s: %s", colName, info, rs.getString(info)));
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

    TableDesc getTableDesc(String schemaName, String tableName) throws Exception {
        final TableDesc tableDesc = new TableDesc(schemaName, tableName);
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "c0", TapDataType.STRING));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "c1", TapDataType.SHORT));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "c2", TapDataType.INTEGER));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "c3", TapDataType.LONG));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "c4", TapDataType.FLOAT));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "c5", TapDataType.DOUBLE));
        tableDesc.getColumnDescs().add(new ColumnDesc(tableName, "c6", TapDataType.TIMESTAMP));
        return tableDesc;
    }

}
