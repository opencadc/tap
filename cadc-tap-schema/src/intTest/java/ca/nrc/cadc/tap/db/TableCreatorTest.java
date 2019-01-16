/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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

package ca.nrc.cadc.tap.db;


import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.util.Log4jInit;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
public class TableCreatorTest {
    private static final Logger log = Logger.getLogger(TableCreatorTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.db", Level.INFO);
    }
    
    private DataSource dataSource;
    private final String TEST_SCHEMA = "tap_schema";
    
    public TableCreatorTest() { 
        // create a datasource and register with JNDI
        try {
            DBConfig conf = new DBConfig();
            ConnectionConfig cc = conf.getConnectionConfig("TAP_SCHEMA_TEST", "cadctest");
            dataSource = DBUtil.getDataSource(cc);
            log.info("configured data source: " + cc.getServer() + "," + cc.getDatabase() + "," + cc.getDriver() + "," + cc.getURL());
        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw new IllegalStateException("failed to create DataSource", ex);
        }
    }
    
    @Test
    public void testCreateTable() {
        try {
            String testTable = TEST_SCHEMA + ".testCreateTable";
            // cleanup
            TableCreator tc = new TableCreator(dataSource);
            tc.dropTable(testTable);
            
            TableDesc orig = new TableDesc(TEST_SCHEMA, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c0", TapDataType.STRING));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c1", TapDataType.SHORT));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c2", TapDataType.INTEGER));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c3", TapDataType.LONG));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c4", TapDataType.FLOAT));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c5", TapDataType.DOUBLE));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c6", TapDataType.TIMESTAMP));
            
            tc.createTable(orig);
            log.info("createTable returned");
            
            String sql = "SELECT * from " + testTable;
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            List<List<Object>> rows = jdbc.query(sql, new SimpleRowMapper());
            Assert.assertNotNull("rows", rows);
            Assert.assertTrue("empty", rows.isEmpty());
            log.info("queries empty table");
            
            // cleanup
            tc.dropTable(testTable);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCreateIndex() {
        try {
            String testTable = TEST_SCHEMA + ".testCreateIndex";
            // cleanup
            TableCreator tc = new TableCreator(dataSource);
            tc.dropTable(testTable);
            
            TableDesc orig = new TableDesc(TEST_SCHEMA, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            ColumnDesc col = new ColumnDesc(testTable, "c0", TapDataType.STRING);
            orig.getColumnDescs().add(col);
            
            tc.createTable(orig);
            log.info("createTable returned");
            
            tc.createIndex(col, false);
            log.info("createIndex returned");
            
            String sql = "SELECT * from " + testTable;
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            List<List<Object>> rows = jdbc.query(sql, new SimpleRowMapper());
            Assert.assertNotNull("rows", rows);
            Assert.assertTrue("empty", rows.isEmpty());
            log.info("queries empty table");
            
            // cleanup
            tc.dropTable(testTable);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCreateUniqueIndex() {
        try {
            String testTable = TEST_SCHEMA + ".testCreateUniqueIndex";
            // cleanup
            TableCreator tc = new TableCreator(dataSource);
            tc.dropTable(testTable);
            
            TableDesc orig = new TableDesc(TEST_SCHEMA, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            ColumnDesc col = new ColumnDesc(testTable, "c0", TapDataType.STRING);
            orig.getColumnDescs().add(col);
            
            tc.createTable(orig);
            log.info("createTable returned");
            
            tc.createIndex(col, true);
            log.info("createIndex returned");
            
            String sql = "SELECT * from " + testTable;
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            List<List<Object>> rows = jdbc.query(sql, new SimpleRowMapper());
            Assert.assertNotNull("rows", rows);
            Assert.assertTrue("empty", rows.isEmpty());
            log.info("queries empty table");
            
            String insert = "INSERT INTO " + testTable + "(c0) values ('foo')";
            int n = jdbc.update(insert);
            Assert.assertEquals("insert", 1, n);
            try {
                n = jdbc.update(insert);
                Assert.fail("expected exception but inserted duplicate value n=" + n);
            } catch (DataIntegrityViolationException expected) {
                log.info("caught expected exception: " + expected);
            }
            
            // cleanup
            tc.dropTable(testTable);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    
    @Test
    public void testInvalidTableName() {
        try {
            String testTable = TEST_SCHEMA + ".testInvalidTableName;drop table tap_schema.tables";
            TableDesc orig = new TableDesc(TEST_SCHEMA, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c0", TapDataType.STRING));
            
            TableCreator tc = new TableCreator(dataSource);
            tc.createTable(orig);
            Assert.fail("expected IllegalArgumentException - createTable returned");
        } catch (IllegalArgumentException expected) {
            log.info("caught expected exception: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testInvalidColumnName() {
        try {
            String testTable = TEST_SCHEMA + ".testInvalidColumnName;drop table tap_schema.tables";
            TableDesc orig = new TableDesc(TEST_SCHEMA, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "c0", TapDataType.STRING));
            
            TableCreator tc = new TableCreator(dataSource);
            tc.createTable(orig);
            Assert.fail("expected IllegalArgumentException - createTable returned");
        } catch (IllegalArgumentException expected) {
            log.info("caught expected exception: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    public static class SimpleRowMapper implements RowMapper {

        @Override
        public Object mapRow(ResultSet rs, int i) throws SQLException {
            int num = rs.getMetaData().getColumnCount();
            List<Object> ret = new ArrayList<Object>(num);
            for (int c = 1; c <= num; c++) {
                ret.add(rs.getObject(c));
            }
            return ret;
        }
        
    }
}
