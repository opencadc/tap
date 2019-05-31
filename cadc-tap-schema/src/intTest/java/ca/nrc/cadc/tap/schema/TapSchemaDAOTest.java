/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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
*  $Revision: 5 $
*
************************************************************************
 */

package ca.nrc.cadc.tap.schema;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.Log4jInit;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class TapSchemaDAOTest {

    private static final Logger log = Logger.getLogger(TapSchemaDAOTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.schema", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db.version", Level.INFO);
    }

    private DataSource dataSource;
    private final String TEST_SCHEMA = "intTest";

    public TapSchemaDAOTest() {
        // create a datasource and register with JNDI
        try {
            DBConfig conf = new DBConfig();
            ConnectionConfig cc = conf.getConnectionConfig("TAP_SCHEMA_TEST", "cadctest");
            dataSource = DBUtil.getDataSource(cc);
            log.info("configured data source: " + cc.getServer() + "," + cc.getDatabase() + "," + cc.getDriver() + "," + cc.getURL());
            
            // init creates the tap_schema tables and populates with self-describing content
            InitDatabaseTS init = new InitDatabaseTS(dataSource, "cadctest", "tap_schema");
            init.doInit();
            
            // add test schema so other test content will satisfy FK constraints
            TapSchemaDAO dao = new TapSchemaDAO();
            dao.setDataSource(dataSource);
            SchemaDesc sd = new SchemaDesc(TEST_SCHEMA);
            dao.put(sd);
        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw new IllegalStateException("failed to create DataSource", ex);
        }
    }

    @Test
    public void testReadTapSchemaSelf() {
        try {
            TapSchemaDAO dao = new TapSchemaDAO();
            dao.setDataSource(dataSource);
            TapSchema ts = dao.get();
            boolean foundTS = false;
            for (SchemaDesc sd : ts.getSchemaDescs()) {
                if (sd.getSchemaName().equalsIgnoreCase("tap_schema")) {
                    foundTS = true;
                    TableDesc ts_schemas = sd.getTable("tap_schema.schemas");
                    Assert.assertNotNull("found tap_schema.schemas", ts_schemas);

                    TableDesc ts_tables = sd.getTable("tap_schema.tables");
                    Assert.assertNotNull("found tap_schema.tables", ts_tables);

                    TableDesc ts_columns = sd.getTable("tap_schema.columns");
                    Assert.assertNotNull("found tap_schema.columns", ts_columns);
                }
            }

            Assert.assertTrue("found tap_schema", foundTS);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetTable() {
        try {
            TapSchemaDAO dao = new TapSchemaDAO();
            dao.setDataSource(dataSource);
            TableDesc td = dao.getTable("tap_schema.tables");
            Assert.assertNotNull(td);
            Assert.assertEquals("tap_schema.tables", td.getTableName());
            Assert.assertTrue("has columns", !td.getColumnDescs().isEmpty());
            Assert.assertTrue("has keys", !td.getKeyDescs().isEmpty());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPutGetDeleteTable() {
        try {
            TapSchemaDAO dao = new TapSchemaDAO();
            dao.setDataSource(dataSource);
            String testTable = TEST_SCHEMA + ".round_trip";
            
            try {
                dao.delete(testTable);
            } catch (ResourceNotFoundException ex) {
                log.debug("table did not exist at setup: " + ex);
            }

            TableDesc td = dao.getTable(testTable);
            Assert.assertNull("initial setup", td);

            TableDesc orig = new TableDesc(TEST_SCHEMA, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c0", TapDataType.STRING));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c1", TapDataType.SHORT));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c2", TapDataType.INTEGER));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c3", TapDataType.LONG));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c4", TapDataType.FLOAT));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c5", TapDataType.DOUBLE));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c6", TapDataType.TIMESTAMP));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c7", TapDataType.INTERVAL));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c8", TapDataType.POINT));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c9", TapDataType.CIRCLE));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c10", TapDataType.POLYGON));

            dao.put(orig);
            td = dao.getTable(testTable);
            Assert.assertNotNull("created table", td);
            Assert.assertEquals("num columns", orig.getColumnDescs().size(), td.getColumnDescs().size());

            dao.delete(td.getTableName());
            td = dao.getTable(testTable);
            Assert.assertNull("delete confirmed", td);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUpdateTable() {
        try {
            TapSchemaDAO dao = new TapSchemaDAO();
            dao.setDataSource(dataSource);
            String testTable = TEST_SCHEMA + ".round_trip";
            
            try {
                dao.delete(testTable);
            } catch (ResourceNotFoundException ex) {
                log.debug("table did not exist at setup: " + ex);
            }

            TableDesc td = dao.getTable(testTable);
            Assert.assertNull("initial setup", td);

            TableDesc orig = new TableDesc(TEST_SCHEMA, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c0", TapDataType.STRING));
            
            dao.put(orig);
            td = dao.getTable(testTable);
            Assert.assertNotNull("created table", td);
            Assert.assertEquals("num columns", orig.getColumnDescs().size(), td.getColumnDescs().size());
            ColumnDesc ecd = td.getColumn("c0");
            Assert.assertNull(ecd.description);
            Assert.assertNull(ecd.unit);
            ecd.description = "new description";
            ecd.unit = "m";
            dao.put(td);
            
            td = dao.getTable(testTable);
            Assert.assertNotNull("modified table", td);
            Assert.assertEquals("num columns", orig.getColumnDescs().size(), td.getColumnDescs().size());
            ColumnDesc acd = td.getColumn("c0");
            Assert.assertEquals("modified description", ecd.description, acd.description);
            Assert.assertEquals("modified unit", ecd.unit, acd.unit);
            
            dao.delete(td.getTableName());
            td = dao.getTable(testTable);
            Assert.assertNull("delete confirmed", td);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testUpdateTableAddColumn() {
        try {
            TapSchemaDAO dao = new TapSchemaDAO();
            dao.setDataSource(dataSource);
            
            String testTable = TEST_SCHEMA + ".testUpdateTableAddColumn";
            try {
                dao.delete(testTable);
            } catch (ResourceNotFoundException ex) {
                log.debug("table did not exist at setup: " + ex);
            }

            TableDesc td = dao.getTable(testTable);
            Assert.assertNull("initial setup", td);

            TableDesc orig = new TableDesc(TEST_SCHEMA, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c0", TapDataType.STRING));
            
            dao.put(orig);
            td = dao.getTable(testTable);
            
            // add column
            Assert.assertNotNull("created table", td);
            Assert.assertEquals("num columns", orig.getColumnDescs().size(), td.getColumnDescs().size());
            td.getColumnDescs().add(new ColumnDesc(testTable, "c1", TapDataType.SHORT));
            try {
                dao.put(td);
                Assert.fail("add column succeeded - expected IllegalArgumentException");
            } catch (UnsupportedOperationException expected) {
                log.info("caught expected exception: " + expected);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testUpdateTableRenameColumn() {
        try {
            TapSchemaDAO dao = new TapSchemaDAO();
            dao.setDataSource(dataSource);
            
            String testTable = TEST_SCHEMA + ".testUpdateTableRenameColumn";
            try {
                dao.delete(testTable);
            } catch (ResourceNotFoundException ex) {
                log.debug("table did not exist at setup: " + ex);
            }

            TableDesc td = dao.getTable(testTable);
            Assert.assertNull("initial setup", td);

            TableDesc orig = new TableDesc(TEST_SCHEMA, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c0", TapDataType.STRING));
            
            dao.put(orig);
            td = dao.getTable(testTable);
            
            // rename column
            td.getColumnDescs().clear();
            td.getColumnDescs().add(new ColumnDesc(testTable, "d0", TapDataType.STRING));
            try {
                dao.put(td);
                Assert.fail("rename column succeeded - expected IllegalArgumentException");
            } catch (UnsupportedOperationException expected) {
                log.info("caught expected exception: " + expected);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testUpdateTableChangeColumnDatatype() {
        try {
            TapSchemaDAO dao = new TapSchemaDAO();
            dao.setDataSource(dataSource);
            
            String testTable = TEST_SCHEMA + ".testUpdateTableChangeColumnDatatype";
            try {
                dao.delete(testTable);
            } catch (ResourceNotFoundException ex) {
                log.debug("table did not exist at setup: " + ex);
            }

            TableDesc td = dao.getTable(testTable);
            Assert.assertNull("initial setup", td);

            TableDesc orig = new TableDesc(TEST_SCHEMA, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c0", TapDataType.STRING));
            
            dao.put(orig);
            td = dao.getTable(testTable);
            
            // change column datatype
            td.getColumnDescs().clear();
            td.getColumnDescs().add(new ColumnDesc(testTable, "c0", TapDataType.INTEGER));
            try {
                dao.put(td);
                Assert.fail("change column datatype succeeded - expected IllegalArgumentException");
            } catch (UnsupportedOperationException expected) {
                log.info("caught expected exception: " + expected);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
}
