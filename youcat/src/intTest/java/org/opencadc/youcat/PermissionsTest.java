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
************************************************************************
*/

package org.opencadc.youcat;

import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.vosi.actions.TableContentHandler;
import java.io.ByteArrayOutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.gms.GroupURI;

/**
 *
 * @author majorb
 */
public class PermissionsTest extends AbstractTablesTest {
    
    private static final Logger log = Logger.getLogger(PermissionsTest.class);
    
    // HACK: this username has to match the schema owner certificate identity
    static final String SCHEMA_OWNER = "cadcauthtest1";
    
    public PermissionsTest() { 
        super();
    }
    
    @Test
    public void testAnon() {
        log.info("testGetAnon()");
        try {
            clearSchemaPerms();
            
            String testTable = testSchemaName + ".testGetAnon";
            doCreateTable(schemaOwner, testTable);
            
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            URL schemaPerms = new URL(permsURL.toString() + "/" + testSchemaName);
            URL tablePerms = new URL(permsURL.toString() + "/" + testTable);
            
            // get schema perms
            HttpDownload get = new HttpDownload(schemaPerms, out);
            get.run();
            Assert.assertNotNull(get.getThrowable());
            Assert.assertEquals(get.getResponseCode(), 403);
            
            // get table perms
            get = new HttpDownload(tablePerms, out);
            get.run();
            Assert.assertNotNull(get.getThrowable());
            Assert.assertEquals(get.getResponseCode(), 403);
            
            String perms =
                "public=true\n" +
                "r-group=ivo://cadc.nrc.ca/gms?testGroup\n" +
                "rw-group=ivo://cadc.nrc.ca/gms?testGroup";
            FileContent content = new FileContent(perms, "text/plain", Charset.forName("utf-8"));

            // set schema perms
            HttpPost post = new HttpPost(schemaPerms, content, false);
            post.run();
            Assert.assertNotNull(post.getThrowable());
            Assert.assertEquals(post.getResponseCode(), 403);
            
            // set table perms
            post = new HttpPost(tablePerms, content, false);
            post.run();
            Assert.assertNotNull(post.getThrowable());
            Assert.assertEquals(post.getResponseCode(), 403);
            
            doDelete(schemaOwner, testTable, false);
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testBadSetParams() {
        log.info("testBadSetParams()");
        try {
            clearSchemaPerms();
            
            String testTable = testSchemaName + ".testBadSetParams";
            doCreateTable(schemaOwner, testTable);
            
            URL tablePerms = new URL(permsURL.toString() + "/" + testTable);
            
            // unknown parameter 
            FileContent content = new FileContent("junk=true", "text/plain", Charset.forName("utf-8"));
            HttpPost post = new HttpPost(tablePerms, content, false);
            Subject.doAs(schemaOwner, new RunnableAction(post));
            Assert.assertNotNull(post.getThrowable());
            Assert.assertEquals(post.getResponseCode(), 400);
            
            // bad value for boolean
            content = new FileContent("public=yes", "text/plain", Charset.forName("utf-8"));
            post = new HttpPost(tablePerms, content, false);
            Subject.doAs(schemaOwner, new RunnableAction(post));
            Assert.assertNotNull(post.getThrowable());
            Assert.assertEquals(post.getResponseCode(), 400);
            
            // bad value for r-group
            content = new FileContent("r-group=notagmsuri", "text/plain", Charset.forName("utf-8"));
            post = new HttpPost(tablePerms, content, false);
            Subject.doAs(schemaOwner, new RunnableAction(post));
            Assert.assertNotNull(post.getThrowable());
            Assert.assertEquals(post.getResponseCode(), 400);
            
            // bad value for rw-group
            content = new FileContent("rw-group=notagmsuri", "text/plain", Charset.forName("utf-8"));
            post = new HttpPost(tablePerms, content, false);
            Subject.doAs(schemaOwner, new RunnableAction(post));
            Assert.assertNotNull(post.getThrowable());
            Assert.assertEquals(post.getResponseCode(), 400);
            
            // cannot set owner
            content = new FileContent("owner=me", "text/plain", Charset.forName("utf-8"));
            post = new HttpPost(tablePerms, content, false);
            Subject.doAs(schemaOwner, new RunnableAction(post));
            Assert.assertNotNull(post.getThrowable());
            Assert.assertEquals(post.getResponseCode(), 400);
            
            doDelete(schemaOwner, testTable, false);
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testPublic() {
        log.info("testPublic()");
        try {
            clearSchemaPerms();
            
            String testTable = testSchemaName + ".testPublic";
            doCreateTable(schemaOwner, testTable);
            
            this.doQuery(anon, anonQueryURL, testTable, 400);
            
            TapPermissions tp = new TapPermissions(null, true, null, null);
            setPerms(schemaOwner, testSchemaName, tp, 200);
            this.doQuery(anon, anonQueryURL, testTable, 403);
            
            setPerms(schemaOwner, testTable, tp, 200);
            this.doQuery(anon, anonQueryURL, testTable, 200);
            
            setPerms(anon, testTable, tp, 403);
            
            doDelete(schemaOwner, testTable, false);
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testGroupRead() {
        log.info("testGroupRead()");
        try {
            
            clearSchemaPerms();
            
            String testTable = testSchemaName + ".testGroupRead";
            doCreateTable(schemaOwner, testTable);
            
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 400);
            this.insertData(subjectWithGroups, certLoadURL, testTable, 403);  
            this.doCreateIndex(subjectWithGroups, testTable, "c0", false, ExecutionPhase.ERROR, "permission denied");
            
            GroupURI readGroup = new GroupURI(VALID_TEST_GROUP);
            TapPermissions tp = new TapPermissions(null, false, readGroup, null);
            setPerms(schemaOwner, testSchemaName, tp, 200);
            TapPermissions tp1 = getPermissions(schemaOwner, testSchemaName, 200);
            Assert.assertNotNull(tp1.isPublic);
            Assert.assertFalse(tp1.isPublic);
            Assert.assertEquals(readGroup, tp1.readGroup);
            Assert.assertNull(tp1.readWriteGroup);
            
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 403);
            this.insertData(subjectWithGroups, certLoadURL, testTable, 403);
            this.doCreateIndex(subjectWithGroups, testTable, "c0", false, ExecutionPhase.ERROR, "permission denied");
            
            setPerms(schemaOwner, testTable, tp, 200);
            tp1 = getPermissions(schemaOwner, testTable, 200);
            Assert.assertNotNull(tp1.isPublic);
            Assert.assertFalse(tp1.isPublic);
            Assert.assertEquals(readGroup, tp1.readGroup);
            Assert.assertNull(tp1.readWriteGroup);
            
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 200);
            this.insertData(subjectWithGroups, certLoadURL, testTable, 403);
            this.doCreateIndex(subjectWithGroups, testTable, "c0", false, ExecutionPhase.ERROR, "permission denied");
            
            doDelete(schemaOwner, testTable, false);
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testGroupReadWrite() {
        log.info("testGroupReadWrite()");
        try {
            clearSchemaPerms();
            
            String testTable = testSchemaName + ".testGroupReadWrite";
            doCreateTable(schemaOwner, testTable);
            
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 400);
            this.insertData(subjectWithGroups, certLoadURL, testTable, 403);
            this.doCreateIndex(subjectWithGroups, testTable, "c0", false, ExecutionPhase.ERROR, "permission denied");;
            
            GroupURI readWriteGroup = new GroupURI(VALID_TEST_GROUP);
            TapPermissions tp = new TapPermissions(null, false, null, readWriteGroup);
            setPerms(schemaOwner, testSchemaName, tp, 200);
            TapPermissions tp1 = getPermissions(schemaOwner, testSchemaName, 200);
            Assert.assertNotNull(tp1.isPublic);
            Assert.assertFalse(tp1.isPublic);
            Assert.assertNull(tp1.readGroup);
            Assert.assertEquals(readWriteGroup, tp1.readWriteGroup);
            
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 403);
            this.insertData(subjectWithGroups, certLoadURL, testTable, 403);
            this.doCreateIndex(subjectWithGroups, testTable, "c0", false, ExecutionPhase.ERROR, "permission denied");
            
            setPerms(schemaOwner, testTable, tp, 200);
            tp1 = getPermissions(schemaOwner, testTable, 200);
            Assert.assertNotNull(tp1.isPublic);
            Assert.assertFalse(tp1.isPublic);
            Assert.assertNull(tp1.readGroup);
            Assert.assertEquals(readWriteGroup, tp1.readWriteGroup);
            
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 200);
            this.insertData(subjectWithGroups, certLoadURL, testTable, 200);
            this.doCreateIndex(subjectWithGroups, testTable, "c0", false, ExecutionPhase.COMPLETED, null);
           
            doDelete(schemaOwner, testTable, false);
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testSchemaOwnerDropTable() {
        log.info("testDropTable()");
        try {
            clearSchemaPerms();
            
            String testTable = testSchemaName + ".testDropTable";
            
            TapPermissions tp = new TapPermissions(null, true, null, new GroupURI(VALID_TEST_GROUP));
            setPerms(schemaOwner, testSchemaName, tp, 200);

            doCreateTable(subjectWithGroups, testTable);
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 200);
            
            doDelete(schemaOwner, testTable, true);
            
            // query should fail with bad request now
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 400);
            
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testDropTable() {
        log.info("testDropTable()");
        try {
            clearSchemaPerms();
            
            String testTable = testSchemaName + ".testDropTable";
            
            TapPermissions tp = new TapPermissions(null, true, null, new GroupURI(VALID_TEST_GROUP));
            setPerms(schemaOwner, testSchemaName, tp, 200);

            doCreateTable(subjectWithGroups, testTable);
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 200);
            this.setPerms(subjectWithGroups, testTable, tp, 200);
            
            doDelete(subjectWithGroups, testTable, true);
            
            // query should fail now
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 400);
            
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testNoInheritance() {
        log.info("testNoInheritance()");
        try {
            
            GroupURI group1 = new GroupURI("ivo://cadc.nrc.ca/gms?group1");
            GroupURI group2 = new GroupURI("ivo://cadc.nrc.ca/gms?group2");
            TapPermissions tp = new TapPermissions(null, true, group1, group2);
            this.setPerms(schemaOwner, testSchemaName, tp, 200);
            
            TapPermissions actual = this.getPermissions(schemaOwner, testSchemaName, 200);
            Assert.assertTrue(actual.owner.getPrincipals(HttpPrincipal.class).iterator().next()
                .getName().equals(SCHEMA_OWNER));
            Assert.assertEquals(true, actual.isPublic);
            Assert.assertEquals(group1, actual.readGroup);
            Assert.assertEquals(group2, actual.readWriteGroup);
            
            String testTable = testSchemaName + ".testNoInheritance";
            doCreateTable(schemaOwner, testTable);
            
            actual = this.getPermissions(schemaOwner, testTable, 200);
            Assert.assertTrue(actual.owner.getPrincipals(HttpPrincipal.class).iterator().next()
                .getName().equals(SCHEMA_OWNER));
            Assert.assertEquals(false, actual.isPublic);
            Assert.assertNull(actual.readGroup);
            Assert.assertNull(actual.readWriteGroup);
            
            doDelete(schemaOwner, testTable, false);
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testQueriesChangingPerms() {
        log.info("testQueriesChangingPerms()");
        try {
            
            clearSchemaPerms();
            
            // query tap_schema.schemas -- null owner so should be public
            this.doQuery(anon, anonQueryURL, "tap_schema.schemas", 200);
            
            String testTable = testSchemaName + ".testQueriesChangingPerms";
            doCreateTable(schemaOwner, testTable);
            
            // initially private
            this.doQuery(anon, certQueryURL, testTable, 400);
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 400);
            this.doQuery(schemaOwner, certQueryURL, testTable, 200);
            
            // set schema and table to public
            TapPermissions tp = new TapPermissions(null, true, null, null);
            this.setPerms(schemaOwner, testSchemaName, tp, 200);
            this.setPerms(schemaOwner, testTable, tp, 200);
            this.doQuery(anon, certQueryURL, testTable, 200);
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 200);
            this.doQuery(schemaOwner, certQueryURL, testTable, 200);
            
            // remove public from table
            tp = new TapPermissions(null, false, null, null);
            this.setPerms(schemaOwner, testTable, tp, 200);
            this.doQuery(anon, certQueryURL, testTable, 403);
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 403);
            this.doQuery(schemaOwner, certQueryURL, testTable, 200);
            
            // add group read
            tp = new TapPermissions(null, false, new GroupURI(VALID_TEST_GROUP), null);
            this.setPerms(schemaOwner, testTable, tp, 200);
            this.doQuery(anon, certQueryURL, testTable, 403);
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 200);
            this.doQuery(schemaOwner, certQueryURL, testTable, 200);
            
            // remove group read, add group read-write
            tp = new TapPermissions(null, false, null, new GroupURI(VALID_TEST_GROUP));
            this.setPerms(schemaOwner, testTable, tp, 200);
            this.doQuery(anon, certQueryURL, testTable, 403);
            this.doQuery(subjectWithGroups, certQueryURL, testTable, 200);
            this.doQuery(schemaOwner, certQueryURL, testTable, 200);
            
            doDelete(schemaOwner, testTable, false);
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testAnonQuerySchemasTable() {
        log.info("testAnonQuerySchemasTable()");
        try {
            clearSchemaPerms();
            
            String query = "select schema_name from tap_schema.schemas";
            
            VOTableDocument doc = doQueryWithResults(anon, anonQueryURL, query);
            
            assertAnonymousSchemaResults(doc);

        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testOwnerQuerySchemasTable() {
        log.info("testOwnerQuerySchemasTable()");
        try {
            clearSchemaPerms();
            
            String query = "select schema_name from tap_schema.schemas";
            
            VOTableDocument doc = doQueryWithResults(schemaOwner, certQueryURL, query);
            
            assertAuthtest1ReadResults(doc);

        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testGroupAccessQuerySchemasTable() {
        log.info("testGroupAccessQuerySchemasTable()");
        try {
            clearSchemaPerms();
            
            GroupURI readGroup = new GroupURI(VALID_TEST_GROUP);
            TapPermissions tp = new TapPermissions(null, false, readGroup, null);
            this.setPerms(this.schemaOwner, testSchemaName, tp, 200);
            
            String query = "select schema_name from tap_schema.schemas";
            
            VOTableDocument doc = doQueryWithResults(subjectWithGroups, certQueryURL, query);
            
            assertAuthtest1ReadResults(doc);

        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testAnonQueryTablesTable() {
        log.info("testAnonQueryTablesTable()");
        try {
            clearSchemaPerms();
            
            String query = "select schema_name from tap_schema.tables";
            VOTableDocument doc = doQueryWithResults(anon, anonQueryURL, query);
            
            assertAnonymousSchemaResults(doc);

        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testOwnerQueryTablesTable() {
        log.info("testOwnerQueryTablesTable()");
        try {
            clearSchemaPerms();

            String testTable = testSchemaName + ".testOwnerQueryTablesTable";
            doCreateTable(schemaOwner, testTable);

            String query = "select schema_name from tap_schema.tables";
            
            VOTableDocument doc = doQueryWithResults(schemaOwner, certQueryURL, query);
            
            assertAuthtest1ReadResults(doc);

            // cleanup on success
            doDelete(schemaOwner, testTable, true);
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testGroupAccessQueryTablesTable() {
        log.info("testGroupAccessQueryTablesTable()");
        try {
            clearSchemaPerms();
            
            String testTable = testSchemaName + ".testGroupAccessQueryTablesTable";
            doCreateTable(schemaOwner, testTable);
            
            GroupURI readGroup = new GroupURI(VALID_TEST_GROUP);
            TapPermissions tp = new TapPermissions(null, false, readGroup, null);
            this.setPerms(this.schemaOwner, testSchemaName, tp, 200);
            
            String query = "select schema_name from tap_schema.tables";
            
            VOTableDocument doc = doQueryWithResults(subjectWithGroups, certQueryURL, query);
            
            assertAuthtest1ReadResults(doc);

            // cleanup on success
            doDelete(schemaOwner, testTable, true);
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testAnonQueryColumnsTable() {
        log.info("testAnonQueryColumnsTable()");
        try {
            clearSchemaPerms();
            
            String query = "select t.schema_name from tap_schema.tables t " +
                "join tap_schema.columns c on t.table_name=c.table_name";
            VOTableDocument doc = doQueryWithResults(anon, anonQueryURL, query);
            
            assertAnonymousSchemaResults(doc);
            
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
        
    }
    
    @Test
    public void testOwnerQueryColumnsTable() {
        log.info("testOwnerQueryColumnsTable()");
        try {
            clearSchemaPerms();
            
            String testTable = testSchemaName + ".testOwnerQueryColumnsTable";
            doCreateTable(schemaOwner, testTable);
            
            String query = "select t.schema_name from tap_schema.tables t" 
                    + " join tap_schema.columns c on t.table_name=c.table_name";
            
            VOTableDocument doc = doQueryWithResults(schemaOwner, certQueryURL, query);
            
            assertAuthtest1ReadResults(doc);

            // cleanup on success
            doDelete(schemaOwner, testTable, true);
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    @Test
    public void testGroupQueryColumnsTable() {
        log.info("testGroupQueryColumnsTable()");
        try {
            clearSchemaPerms();
            
            GroupURI readGroup = new GroupURI(VALID_TEST_GROUP);
            TapPermissions tp = new TapPermissions(null, false, readGroup, null);
            this.setPerms(this.schemaOwner, testSchemaName, tp, 200);
            
            String testTable = testSchemaName + ".testGroupQueryColumnsTable";
            doCreateTable(schemaOwner, testTable);
            
            String query = "select t.schema_name from tap_schema.tables t " +
                "join tap_schema.columns c on t.table_name=c.table_name";
            
            VOTableDocument doc = doQueryWithResults(subjectWithGroups, certQueryURL, query);
            
            assertAuthtest1ReadResults(doc);

            // cleanup on success
            doDelete(schemaOwner, testTable, true);
        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    private void assertAnonymousSchemaResults(VOTableDocument doc) {
        
        try {
            
            VOTableResource results = doc.getResourceByType("results");
            VOTableTable table = results.getTable();
            TableData data = table.getTableData();
            Iterator<List<Object>> rows = data.iterator();
            if (!rows.hasNext()) {
                Assert.fail("no schema rows returned");
            }
            
            // expect at least:
            //  - the tap_schema schema to be present
            //  - the cadcauthtest1 schema to be not present
            boolean foundTapSchemaSchema = false;
            boolean foundCadcauthtest1Schema = false;
            while (rows.hasNext()) {
                List<Object> row = rows.next();
                if (((String) row.get(0)).equals("tap_schema")) {
                    foundTapSchemaSchema = true;
                }
                if (((String) row.get(0)).equals(testSchemaName)) {
                    foundCadcauthtest1Schema = true;
                }
            }
            if (!foundTapSchemaSchema) {
                Assert.fail("failed to find tap schema schema");
            }
            if (foundCadcauthtest1Schema) {
                Assert.fail("mistakenly found " + testSchemaName + " schema");
            }

        } catch (Throwable t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
    
    private void assertAuthtest1ReadResults(VOTableDocument doc) {
        
        try {
            VOTableResource results = doc.getResourceByType("results");
            VOTableTable table = results.getTable();
            TableData data = table.getTableData();
            Iterator<List<Object>> rows = data.iterator();
            if (!rows.hasNext()) {
                Assert.fail("no schema rows returned");
            }
            
            // expect at least:
            //  - the tap_schema schema to be present
            //  - the cadcauthtest1 schema to be present
            boolean foundTapSchemaSchema = false;
            boolean foundCadcauthtest1Schema = false;
            while (rows.hasNext()) {
                List<Object> row = rows.next();
                if (((String) row.get(0)).equals("tap_schema")) {
                    foundTapSchemaSchema = true;
                }
                if (((String) row.get(0)).equals(testSchemaName)) {
                    foundCadcauthtest1Schema = true;
                }
            }
            if (!foundTapSchemaSchema) {
                Assert.fail("failed to find tap schema schema");
            }
            if (!foundCadcauthtest1Schema) {
                Assert.fail("failed to find " + testSchemaName + " schema");
            }
        } catch (Throwable t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        }
    }
        
    private TapPermissions getPermissions(Subject subject, String name, int expectedCode) throws MalformedURLException {

        URL getPermsURL = new URL(permsURL.toString() + "/" + name);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpDownload get = new HttpDownload(getPermsURL, out);
        Subject.doAs(subject, new RunnableAction(get));
        Assert.assertEquals(get.getResponseCode(), expectedCode);
        if (get.getResponseCode() == 200) {
            String response = out.toString();
            log.info("get perms response: " + response);
            String[] lines = response.split("\n");
            String ownerString = null;
            Boolean isPublic = null;
            GroupURI readGroup = null;
            GroupURI readWriteGroup = null;
            for (String next : lines) {
                String[] parts = next.split("[=]");
                boolean hasValue = parts.length >= 2;
                String key = parts[0];
                String value = next.substring(key.length() + 1);
                if (key.equals("owner")) {
                    if (hasValue) {
                        ownerString = value;
                    }
                }
                if (key.equals("public")) {
                    if (hasValue) {
                        isPublic = Boolean.parseBoolean(value);
                    } else {
                        throw new RuntimeException("cannot have null public value");
                    }
                }
                if (key.equals("r-group")) {
                    if (hasValue) {
                        readGroup = new GroupURI(URI.create(value));
                    }
                }
                if (key.equals("rw-group")) {
                    if (hasValue) {
                        readWriteGroup = new GroupURI(URI.create(value));
                    }
                }
            }
            
            Subject owner = new Subject();
            if (ownerString != null) {
                // username from IdentityManager.toDisplayString(Subject)
                HttpPrincipal p = new HttpPrincipal(ownerString);
                owner.getPrincipals().add(p);
            }
            return new TapPermissions(owner, isPublic, readGroup, readWriteGroup);
        }
        return null;
    }
    
    private Integer doQuery(Subject subject, URL url, String testTable, Integer expectedCode) throws Exception {
        String adql = "SELECT * from " + testTable;
        return doQuery(subject, url, adql, testTable, expectedCode);
    }
    
    private Integer doQuery(Subject subject, URL url, String adql, String testTable, Integer expectedCode) throws Exception {
        Map<String, Object> params = new TreeMap<String, Object>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        params.put("RESPONSEFORMAT", "CSV");
        Integer respCode = null;
        log.info("Performing query on URL: " + url);
        SyncQueryAction query = new SyncQueryAction(url, params);
        if (subject != null) {
            respCode = Subject.doAs(subject, query);
        } else {
            respCode = query.run();
        }
        log.info("Query response code: " + respCode);
        Assert.assertEquals(expectedCode, respCode);
        return respCode;
    }
    
    class SyncQueryAction implements PrivilegedExceptionAction<Integer> {

        private URL url;
        private Map<String, Object> params;

        public SyncQueryAction(URL url, Map<String, Object> params) {
            this.url = url;
            this.params = params;
        }

        @Override
        public Integer run()
                throws Exception {
            HttpPost doit = new HttpPost(url, params, true);
            doit.run();
            
            if (doit.getThrowable() != null) {
                log.info("Throwable on sync query: " + doit.getThrowable());
            }

            int code = doit.getResponseCode();
            return code;
        }
    }
    
    private VOTableDocument doQueryWithResults(Subject subject, URL url, String adql) throws Exception {
        Map<String, Object> params = new TreeMap<String, Object>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        VOTableDocument ret = null;
        log.info("Performing query on URL: " + url);
        SyncQueryActionWithResults query = new SyncQueryActionWithResults(url, params);
        if (subject != null) {
            ret = Subject.doAs(subject, query);
        } else {
            ret = query.run();
        }
        return ret;
    }
    
    class SyncQueryActionWithResults implements PrivilegedExceptionAction<VOTableDocument> {

        private URL url;
        private Map<String, Object> params;

        public SyncQueryActionWithResults(URL url, Map<String, Object> params) {
            this.url = url;
            this.params = params;
        }

        @Override
        public VOTableDocument run()
                throws Exception {
            HttpPost doit = new HttpPost(url, params, true);
            doit.prepare();
            
            if (doit.getThrowable() != null) {
                log.info("Throwable on sync query: " + doit.getThrowable());
            }

            int code = doit.getResponseCode();
            if (code != 200) {
                throw new RuntimeException(Integer.toString(code));
            }
            
            VOTableReader reader = new VOTableReader();
            //String xml = doit.getResponseBody();
            //log.info("xml: " + xml);
            VOTableDocument doc = reader.read(doit.getInputStream());
            
            return doc;
        }
    }    
    private void insertData(Subject subject, URL url, String testTable, int expectedCode) throws Exception {
        StringBuilder data = new StringBuilder();
        data.append("c0\tc1\tc2\tc3\tc4\tc5\tc6\te7\te8\te9\te10\n");
        for (int i = 0; i < 10; i++) {
            data.append("string" + i).append("\t");
            data.append(Short.MAX_VALUE).append("\t");
            data.append(Integer.MAX_VALUE).append("\t");
            data.append(Long.MAX_VALUE).append("\t");
            data.append(Float.MAX_VALUE).append("\t");
            data.append(Double.MAX_VALUE).append("\t");
            data.append("2018-11-05T22:12:33.111").append("\t");
            data.append("1.0 2.0").append("\t");  // interval
            data.append("1.0 2.0").append("\t");  // point
            data.append("1.0 2.0 3.0").append("\t");  // circle
            data.append("1.0 2.0 3.0 4.0 5.0 6.0").append("\n");  // polygon
        }
        insertData(subject, url, data.toString(), testTable, expectedCode);
    }
    
    private void insertData(Subject subject, URL url, String data, String testTable, int expectedCode) throws Exception {
        URL postURL = new URL(url.toString() + "/" + testTable);
        log.info("Posting table data to: " + postURL);
        FileContent content = new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, Charset.forName("utf-8"));
        final HttpPost post = new HttpPost(postURL, content, false);
        Subject.doAs(subject, new RunnableAction(post));
        Assert.assertEquals(expectedCode, post.getResponseCode());
    }
    
}
