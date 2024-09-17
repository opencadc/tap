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

package org.opencadc.youcat;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class TableUpdateTest extends AbstractTablesTest {
    private static final Logger log = Logger.getLogger(TableUpdateTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.youcat", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
    }

    private final DataSource dataSource;
    private final TapSchemaDAO tapSchemaDAO;

    public TableUpdateTest() {
        super();

        try {
            DBConfig conf = new DBConfig();
            ConnectionConfig cc = conf.getConnectionConfig("YOUCAT_TEST", "cadctest");
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
    public void testCreateIndex() {
        try {
            clearSchemaPerms();
            TapPermissions tp = new TapPermissions(null, true, null, null);
            super.setPerms(schemaOwner, testSchemaName, tp, 200);

            String tableName = testSchemaName + ".testCreateIndex";
            TableDesc td = doCreateTable(schemaOwner, tableName);
            for (ColumnDesc cd : td.getColumnDescs()) {
                log.info("testCreateIndex: " + cd.getColumnName());
                ExecutionPhase expected = ExecutionPhase.COMPLETED;
                if (cd.getColumnName().startsWith("a")) {
                    expected = ExecutionPhase.ERROR;
                }
                doCreateIndex(schemaOwner, tableName, cd.getColumnName(), false,expected, null);
            }

            // cleanup on success
            doDelete(schemaOwner, td.getTableName(), false);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCreateUniqueIndex() {
        try {
            clearSchemaPerms();
            TapPermissions tp = new TapPermissions(null, true, null, null);
            super.setPerms(schemaOwner, testSchemaName, tp, 200);

            String tableName = testSchemaName + ".testCreateUniqueIndex";
            TableDesc td = doCreateTable(schemaOwner, tableName);
            for (ColumnDesc cd : td.getColumnDescs()) {

                ExecutionPhase expected = ExecutionPhase.COMPLETED;
                if (cd.getColumnName().startsWith("e") || cd.getColumnName().startsWith("a")) {
                    expected = ExecutionPhase.ERROR; // unique index not allowed
                }
                log.info("testCreateUniqueIndex: " + cd.getColumnName() + " expect: " + expected.getValue());
                doCreateIndex(schemaOwner, tableName, cd.getColumnName(), true, expected, null);
            }

            // cleanup on success
            doDelete(schemaOwner, tableName, false);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testIngestTable() {
        try {
            clearSchemaPerms();
            TapPermissions tp = new TapPermissions(null, true, null, null);
            super.setPerms(schemaOwner, testSchemaName, tp, 200);

            final String testTable = testSchemaName + ".test_ingest_table";
            
            // delete the table from the database: 
            // !apiCreated so cleanup in doCreateTable is not complete
            try {
                String drop = "DROP TABLE " + testTable;
                JdbcTemplate jdbc = new JdbcTemplate(dataSource);
                jdbc.execute(drop);
                log.info("successfully dropped: " + testTable);
            } catch (Exception ignore) {
                log.debug("ingest-table cleanup-before-test failed for " + testTable);
            }
            
            // create test table and schema
            final TableDesc orig = doCreateTable(schemaOwner, testTable);

            // delete the table from tap_schema so we can ingest it
            try {
                tapSchemaDAO.delete(testTable);
            } catch (Exception ignore) {
                log.debug("tap_schema cleanup-before-test failed for " + testTable);
            }

            // run the ingest
            doIngestTable(schemaOwner, testTable, ExecutionPhase.COMPLETED);
            
            TableDesc td = tapSchemaDAO.getTable(testTable);
            Assert.assertNotNull(td);
            log.info("found: " + td.getTableName() + " apiCreated=" + td.apiCreated);
            Assert.assertEquals(orig.getColumnDescs().size(), td.getColumnDescs().size());
            for (ColumnDesc ocd : orig.getColumnDescs()) {
                ColumnDesc cd = td.getColumn(ocd.getColumnName());
                Assert.assertNotNull(ocd.getColumnName(), cd);
                Assert.assertEquals(ocd.getDatatype(), cd.getDatatype()); // TapDataType.equals() is lax
                log.info("found: " + cd.getColumnName() + " " + cd.getDatatype());
            }
            
            // cleanup on success
            doDelete(schemaOwner, testTable, false);
            
            // delete the table from the database: 
            // !apiCreated so cleanup in doCreateTable is not complete
            try {
                String drop = "DROP TABLE " + testTable;
                JdbcTemplate jdbc = new JdbcTemplate(dataSource);
                jdbc.execute(drop);
                log.info("successfully dropped: " + testTable);
            } catch (Exception ignore) {
                log.debug("ingest-table cleanup-before-test failed for " + testTable);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    // test only runs using a PostgreSQL database because
    // it is using a pg specific unsupported datatype.
    @Test
    public void testIngestUnsupportedDataType() {
        try {
            DatabaseMetaData databaseMetaData = dataSource.getConnection().getMetaData();
            String dbProductName = databaseMetaData.getDatabaseProductName();
            if (!"PostgreSQL".equals(dbProductName)) {
                log.info("expected PostgreSQL database, found unsupported database: " + dbProductName);
                return;
            }

            // create test table in the database with an unsupported data type
            String testTable = testSchemaName + ".test_unsupported_datatype";
            String unsupportedDataType = "money";
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            final String drop = String.format("DROP TABLE %s", testTable);
            try {
                jdbc.execute(drop);
            } catch (Exception ignore) {
                log.info("ignore: " + ignore);
            }
            String sql = String.format("CREATE TABLE %s (c1 varchar(16), i1 integer, m1 %s)",
                    testTable, unsupportedDataType);
            log.debug("sql:\n" + sql);
            try {
                jdbc.execute(sql);
            } catch (Exception e) {
                Assert.fail(String.format("error creating table %s because %s", testTable, e.getMessage()));
            }
            log.debug("created database table: " + testTable);

            try {
                doIngestTable(schemaOwner, testTable, ExecutionPhase.ERROR);
            } catch (UnsupportedOperationException expected) {
                log.info("expected exception: " + expected);
            }
            
            TableDesc td = tapSchemaDAO.getTable(testTable);
            Assert.assertNull(td);

            try {
                jdbc.execute(drop);
            } catch (Exception ignore) {
                log.info("ignore: " + ignore);
            }
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    void doIngestTable(Subject subject, String tableName, ExecutionPhase expected) throws Exception {
        // create job
        Map<String,Object> params = new TreeMap<String,Object>();
        params.put("ingest", "true");
        params.put("table", tableName);
        HttpPost post = new HttpPost(certUpdateURL, params, false);
        Subject.doAs(subject, new RunnableAction(post));
        Assert.assertNull("throwable", post.getThrowable());
        Assert.assertEquals("response code", 303, post.getResponseCode());
        final URL jobURL = post.getRedirectURL();
        Assert.assertNotNull("jobURL", jobURL);
        log.info("ingest table job: " + jobURL);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(jobURL, bos);
        Subject.doAs(subject, new RunnableAction(get));
        Assert.assertNull("throwable", get.getThrowable());
        Assert.assertEquals("response code", 200, get.getResponseCode());
        String xml = bos.toString();
        log.debug("ingest table job:\n" + xml);

        // execute job
        params.clear();
        params.put("PHASE", "RUN");
        final URL phaseURL = new URL(jobURL.toExternalForm() + "/phase");
        post = new HttpPost(phaseURL, params, false);
        Subject.doAs(subject, new RunnableAction(post));
        Assert.assertNull("throwable", post.getThrowable());

        // wait for completion
        bos.reset();
        final URL blockURL = new URL(jobURL.toExternalForm() + "?WAIT=60");
        HttpGet block = new HttpGet(blockURL, bos);
        Subject.doAs(subject, new RunnableAction(block));
        Assert.assertNull("throwable", block.getThrowable());
        Assert.assertEquals("response code", 200, block.getResponseCode());
        String xml2 = bos.toString();
        log.debug("final job state:\n" + xml2);

        JobReader r = new JobReader();
        Job end = r.read(new StringReader(xml2));
        Assert.assertEquals("final job state", expected, end.getExecutionPhase());
    }

}
