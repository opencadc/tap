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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.vosi.InvalidTableSetException;
import ca.nrc.cadc.vosi.TableReader;
import ca.nrc.cadc.vosi.TableSetReader;
import ca.nrc.cadc.vosi.TableWriter;
import ca.nrc.cadc.vosi.actions.TableContentHandler;
import ca.nrc.cadc.vosi.actions.TablesInputHandler;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.opencadc.tap.TapClient;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * base class with common setup and methods.
 * 
 * @author pdowler
 */
abstract class AbstractTablesTest {
    private static final Logger log = Logger.getLogger(AbstractTablesTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.youcat", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
    }
    
    static final String YOUCAT_ADMIN = "youcat-admin.pem";         // to create test schema
    static final String SCHEMA_OWNER_CERT = "youcat-owner.pem";    // own test schema
    static final String SCHEMA_GROUP_MEMBER = "youcat-member.pem"; // member of group
    
    static String VALID_TEST_GROUP = "ivo://cadc.nrc.ca/gms?YouCat-ReadWrite";

    Subject admin;
    Subject anon;
    Subject schemaOwner;
    Subject subjectWithGroups;
    
    protected String testSchemaName = "int_test_schema";
    protected final String testCreateSchema = "test_create_schema";
    
    URL anonQueryURL;
    URL certQueryURL;
    URL anonTablesURL;
    URL certTablesURL;
    URL certUpdateURL;
    URL certLoadURL;
    URL permsURL;

    public static final UUID STATIC_UUID = UUID.fromString("cd10277a-3246-4bd0-aa8f-d6aa718b250f");

    AbstractTablesTest() { 
        try {
            anon = AuthenticationUtil.getAnonSubject();
            
            File cf = FileUtil.getFileFromResource(YOUCAT_ADMIN, AbstractTablesTest.class);
            admin = SSLUtil.createSubject(cf);
            log.debug("created admin: " + admin);
            
            cf = FileUtil.getFileFromResource(SCHEMA_OWNER_CERT, AbstractTablesTest.class);
            schemaOwner = SSLUtil.createSubject(cf);
            anon = AuthenticationUtil.getAnonSubject();
            log.debug("created schemaOwner: " + schemaOwner);
            
            cf = FileUtil.getFileFromResource(SCHEMA_GROUP_MEMBER, AbstractTablesTest.class);
            subjectWithGroups = SSLUtil.createSubject(cf);
            // HACK: need this for an ownership test to work
            subjectWithGroups.getPrincipals().add(new HttpPrincipal("cadcauthtest2")); 
            log.debug("created subjectWithGroups: " + subjectWithGroups);

            try {
                RegistryClient reg = new RegistryClient();
                TapClient tc = new TapClient(Constants.RESOURCE_ID);
                anonQueryURL = tc.getSyncURL(Standards.SECURITY_METHOD_ANON);
                certQueryURL = tc.getSyncURL(Standards.SECURITY_METHOD_CERT);
                anonTablesURL = reg.getServiceURL(Constants.RESOURCE_ID, Standards.VOSI_TABLES_11, AuthMethod.ANON);
                certTablesURL = reg.getServiceURL(Constants.RESOURCE_ID, Standards.VOSI_TABLES_11, AuthMethod.CERT);
                certUpdateURL = reg.getServiceURL(Constants.RESOURCE_ID, Standards.PROTO_TABLE_UPDATE_ASYNC, AuthMethod.CERT);
                certLoadURL = reg.getServiceURL(Constants.RESOURCE_ID, Standards.PROTO_TABLE_LOAD_SYNC, AuthMethod.CERT);
                permsURL = reg.getServiceURL(Constants.RESOURCE_ID, Standards.PROTO_TABLE_PERMISSIONS, AuthMethod.CERT);
            } catch (Exception ex) {
                log.error("TEST SETUP BUG: failed to find TAP URL", ex);
            }
            
            // TODO: use youcat-admin to create the test schema owned by youcat-owner
            
        } catch (Throwable t) {
            throw new RuntimeException("TEST SETUP FAILED", t);
        }
    }

    protected void checkTestSchema(String name) {
        if (name.equals(testCreateSchema) || name.equals(testSchemaName) || name.startsWith(testSchemaName + ".")) {
            return; // ok
        }
        throw new RuntimeException("TEST BUG: attempt to use schema|table name " + name
                + " not in test schema " + testSchemaName);
    }
    
    void doDelete(Subject subject, String tableName, boolean fnf) throws Exception {
        checkTestSchema(tableName);
        
        URL tableURL = new URL(certTablesURL.toExternalForm() + "/" + tableName);
        
        // delete
        HttpDelete del = new HttpDelete(tableURL, false);
        log.info("doDelete: " + tableURL);
        Subject.doAs(subject, new RunnableAction(del));
        if (fnf) {
            return;
        }
        
        // check if it worked
        log.info("doDelete: verifying...");
        if (del.getThrowable() != null && del.getThrowable() instanceof Exception) {
            throw (Exception) del.getThrowable();
        }

        HttpGet check = new HttpGet(tableURL, new ByteArrayOutputStream());
        Subject.doAs(subject, new RunnableAction(check));
        Assert.assertEquals("table deleted", 404, check.getResponseCode());
    }
    
    TableDesc doCreateTable(Subject subject, String tableName) throws Exception {
        checkTestSchema(tableName);

        // cleanup just in case
        doDelete(subject, tableName, true);

        final TableDesc orig = new TableDesc(testSchemaName, tableName);
        orig.description = "created by intTest";
        orig.tableType = TableDesc.TableType.TABLE;
        orig.tableIndex = 1;
        orig.getColumnDescs().add(new ColumnDesc(tableName, "c0", TapDataType.STRING));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "c1", TapDataType.SHORT));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "c2", TapDataType.INTEGER));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "c3", TapDataType.LONG));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "c4", TapDataType.FLOAT));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "c5", TapDataType.DOUBLE));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "c6", TapDataType.TIMESTAMP));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "e7", TapDataType.INTERVAL));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "e8", TapDataType.POINT));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "e9", TapDataType.CIRCLE));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "e10", TapDataType.POLYGON));
        // arrays
        orig.getColumnDescs().add(new ColumnDesc(tableName, "a11", new TapDataType("short", "*", null)));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "a12", new TapDataType("int", "*", null)));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "a13", new TapDataType("long", "*", null)));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "a14", new TapDataType("float", "*", null)));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "a15", new TapDataType("double", "*", null)));

        orig.getColumnDescs().add(new ColumnDesc(tableName, "e16", TapDataType.URI));
        orig.getColumnDescs().add(new ColumnDesc(tableName, "e17", new TapDataType("char","36", "uuid")));

        // create
        URL tableURL = new URL(certTablesURL.toExternalForm() + "/" + tableName);
        TableWriter w = new TableWriter();
        StringWriter sw = new StringWriter();
        w.write(orig, sw);
        log.info("VOSI-table description:\n" + sw.toString());
        
        OutputStreamWrapper src = new OutputStreamWrapper() {
            @Override
            public void write(OutputStream out) throws IOException {
                TableWriter w = new TableWriter();
                w.write(orig, new OutputStreamWriter(out));
            }
        };
        HttpUpload put = new HttpUpload(src, tableURL);
        put.setContentType(TablesInputHandler.VOSI_TABLE_TYPE);
        log.info("doCreateTable: " + tableURL);
        Subject.doAs(subject, new RunnableAction(put));
        log.info("doCreateTable: " + put.getResponseCode());
        if (put.getThrowable() != null && put.getThrowable() instanceof Exception) {
            throw (Exception) put.getThrowable();
        }
        Assert.assertEquals("response code", 200, put.getResponseCode());
        return orig;
    }
    
    void doCreateIndex(Subject subject, String tableName, String indexCol, boolean unique, ExecutionPhase expected, String emsg) throws Exception {
        checkTestSchema(tableName);

        Assert.assertNotNull("found async table-update URL", certUpdateURL);
        
        // create job
        Map<String,Object> params = new TreeMap<String,Object>();
        params.put("index", indexCol);
        params.put("table", tableName);
        params.put("unique", Boolean.toString(unique));
        HttpPost post = new HttpPost(certUpdateURL, params, false);
        post.setMaxRetries(0); // testing read-only and offline mode
        Subject.doAs(subject, new RunnableAction(post));
        Assert.assertNull("throwable", post.getThrowable());
        Assert.assertEquals("response code", 303, post.getResponseCode());
        final URL jobURL = post.getRedirectURL();
        Assert.assertNotNull("jobURL", jobURL);
        log.info("create index job: " + jobURL);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(jobURL, bos);
        Subject.doAs(subject, new RunnableAction(get));
        Assert.assertNull("throwable", get.getThrowable());
        Assert.assertEquals("response code", 200, get.getResponseCode());
        String xml = bos.toString();
        log.debug("create index job:\n" + xml);

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
        if (emsg != null) {
            Assert.assertTrue("error message", end.getErrorSummary().getSummaryMessage().startsWith(emsg));
        }
    }
    
    protected void clearSchemaPerms() throws MalformedURLException {
        TapPermissions tp = new TapPermissions();
        tp.isPublic = false;
        setPerms(schemaOwner, testSchemaName, tp, 200);
    }
    
    protected void setPerms(Subject subject, String name, TapPermissions tp, int expectedCode) throws MalformedURLException {
        checkTestSchema(name);
        
        StringBuilder perms = new StringBuilder();
        if (tp.isPublic != null) {
            perms.append("public=").append(Boolean.toString(tp.isPublic)).append("\n");
        } else {
            perms.append("public=false\n");
        }
        if (tp.readGroup != null) {
            perms.append("r-group=").append(tp.readGroup.getURI()).append("\n");
        } else {
            perms.append("r-group=\n");
        }
        if (tp.readWriteGroup != null) {
            perms.append("rw-group=").append(tp.readWriteGroup.getURI()).append("\n");
        } else {
            perms.append("rw-group=\n");
        }
        
        log.info("Setting perms:\n" + perms);
        
        FileContent content = new FileContent(perms.toString(), "text/plain", Charset.forName("utf-8"));
        URL schemaURL = new URL(permsURL.toString() + "/" + name);
        HttpPost post = new HttpPost(schemaURL, content, false);
        post.setMaxRetries(0); // testing read-only and offline mode
        Subject.doAs(subject, new RunnableAction(post));
        Assert.assertEquals(expectedCode, post.getResponseCode());
        
    }

    protected void compare(TableDesc expected, TableDesc actual) {
        // When you read just a single table document you do not get the schema name and TableReader makes one up
        //Assert.assertEquals("schema name", "default", actual.getSchemaName());

        Assert.assertEquals("table name", expected.getTableName(), actual.getTableName());
        Assert.assertEquals("table description", expected.description, actual.description);

        Assert.assertEquals("num columns", expected.getColumnDescs().size(), actual.getColumnDescs().size());
        for (int i = 0; i < expected.getColumnDescs().size(); i++) {
            ColumnDesc ecd = expected.getColumnDescs().get(i);
            ColumnDesc acd = actual.getColumnDescs().get(i);
            Assert.assertEquals("column:table name", ecd.getTableName(), acd.getTableName());
            Assert.assertEquals("column name", ecd.getColumnName(), acd.getColumnName());
            Assert.assertEquals("column datatype", ecd.getDatatype(), acd.getDatatype());
            Assert.assertEquals("column description", ecd.description, acd.description);
            //Assert.assertEquals("column principal", ecd.principal, acd.principal);
            //Assert.assertEquals("column std", ecd.std, acd.std);
            Assert.assertEquals("column columnID", ecd.columnID, acd.columnID);
        }
    }

    protected void compare(VOTableTable expected, VOTableTable actual) {
        // no table name or table description
        for (int i = 0; i < expected.getFields().size(); i++) {
            VOTableField ef = expected.getFields().get(i);
            VOTableField af = actual.getFields().get(i);
            Assert.assertEquals("column name", ef.getName(), af.getName());
            Assert.assertEquals("column datatype", ef.getDatatype(), af.getDatatype());
            Assert.assertEquals("column arraysize", ef.getArraysize(), af.getArraysize());
            Assert.assertEquals("column xtype", ef.xtype, af.xtype);
            Assert.assertEquals("column description", ef.description, af.description);
        }
    }

    protected static class StreamTableReader implements InputStreamWrapper {
        TableDesc td;

        @Override
        public void read(InputStream in) throws IOException {
            try {
                TableReader r = new  TableReader(false); // schema validation causes default arraysize="1" to be injected
                td = r.read(in);
            } catch (
                    InvalidTableSetException ex) {
                throw new RuntimeException("invalid table metadata: ", ex);
            }
        }
    }

    protected static class StreamSchemaReader implements InputStreamWrapper {
        List<SchemaDesc> schemas;

        @Override
        public void read(InputStream in) throws IOException {
            try {
                TableSetReader r = new TableSetReader();
                TapSchema ts = r.read(in);
                this.schemas = ts.getSchemaDescs();
            } catch (InvalidTableSetException ex) {
                throw new RuntimeException("invalid table metadata: ", ex);
            }
        }
    }

    protected SchemaDesc doVosiSchemaCheck(Subject caller, String schemaName) throws Exception {
        // VOSI tables check (metadata)
        URL getTableURL = new URL(anonTablesURL.toExternalForm() + "/" + schemaName);
        AbstractTablesTest.StreamSchemaReader isw = new AbstractTablesTest.StreamSchemaReader();
        HttpGet get = new HttpGet(getTableURL, isw);
        log.info("doVosiCheck: " + getTableURL);
        Subject.doAs(caller, new RunnableAction(get));
        log.info("doVosiCheck: " + get.getResponseCode());
        Assert.assertNull("throwable", get.getThrowable());
        Assert.assertEquals("response code", 200, get.getResponseCode());
        SchemaDesc sd = null;
        for (SchemaDesc s : isw.schemas) {
            if (schemaName.equals(s.getSchemaName())) {
                sd = s;
            }
        }
        Assert.assertNotNull(sd);
        return sd;
    }

    protected TableDesc doVosiCheck(String testTable) throws Exception {
        // VOSI tables check (metadata)
        URL getTableURL = new URL(anonTablesURL.toExternalForm() + "/" + testTable);
        AbstractTablesTest.StreamTableReader isw = new AbstractTablesTest.StreamTableReader();
        HttpDownload get = new HttpDownload(getTableURL, isw);
        log.info("doVosiCheck: " + getTableURL);
        get.run(); // anon
        log.info("doVosiCheck: " + get.getResponseCode());
        Assert.assertNull("throwable", get.getThrowable());
        Assert.assertEquals("response code", 200, get.getResponseCode());
        TableDesc td = isw.td;
        Assert.assertNotNull(td);
        return td;
    }

    public void assertEquals(DoubleInterval expected, DoubleInterval actual) {
        Assert.assertEquals(expected.getLower(), actual.getLower(), 1.0e-9);
        Assert.assertEquals(expected.getUpper(), actual.getUpper(), 1.0e-9);

    }

    public void assertEquals(Point expected, Point actual) {
        Assert.assertEquals(expected.getLongitude(), actual.getLongitude(), 1.0e-9);
        Assert.assertEquals(expected.getLatitude(), actual.getLatitude(), 1.0e-9);
    }

    public void assertEquals(Circle expected, Circle actual) {
        assertEquals(expected.getCenter(), actual.getCenter());
        Assert.assertEquals(expected.getRadius(), actual.getRadius(), 1.0e-9);
    }

    public void assertEquals(Polygon expected, Polygon actual) {
        Assert.assertEquals("num vertices", expected.getVertices().size(), actual.getVertices().size());
        for (int i=0; i < expected.getVertices().size(); i++) {
            Point ep = expected.getVertices().get(i);
            Point ap = actual.getVertices().get(i);
            assertEquals(ep, ap);
        }
    }

    public static String doPrepareDataAllDataTypesTSV() throws URISyntaxException {
        StringBuilder data = new StringBuilder();
        data.append("c0\tc1\tc2\tc3\tc4\tc5\tc6\te7\te8\te9\te10\ta11\ta12\ta13\ta14\ta15\te16\te17\n");
        for (int i = 0; i < 10; i++) {
            data.append("string").append(i).append("\t");
            data.append(Short.MAX_VALUE).append("\t");
            data.append(Integer.MAX_VALUE).append("\t");
            data.append(Long.MAX_VALUE).append("\t");
            data.append(Float.MAX_VALUE).append("\t");
            data.append(Double.MAX_VALUE).append("\t");
            data.append("2018-11-05T22:12:33.111").append("\t");
            data.append("1.0 2.0").append("\t");  // interval
            data.append("1.0 2.0").append("\t");  // point
            data.append("1.0 2.0 3.0").append("\t");  // circle
            data.append("1.0 2.0 3.0 4.0 5.0 6.0").append("\t");  // polygon
            data.append(Short.MIN_VALUE + " " + Short.MAX_VALUE).append("\t");
            data.append(Integer.MIN_VALUE + " " + Integer.MAX_VALUE).append("\t");
            data.append(Long.MIN_VALUE + " " + Long.MAX_VALUE).append("\t");
            data.append(Float.MIN_VALUE + " " + Float.MAX_VALUE).append("\t");
            data.append(Double.MIN_VALUE + " " + Double.MAX_VALUE).append("\t");
            data.append(new URI("ivo://opencadc.org/youcat")).append("\t");
            data.append(STATIC_UUID).append("\n");
        }
        return data.toString();
    }

    public void doUploadTSVData(String testTable, String data) throws MalformedURLException, PrivilegedActionException {
        URL postURL = new URL(certLoadURL.toString() + "/" + testTable);
        final HttpPost post = new HttpPost(postURL, new FileContent(data, TableContentHandler.CONTENT_TYPE_TSV, UTF_8), false);
        Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                post.run();
                return null;
            }
        });
        Assert.assertNull(post.getThrowable());
        Assert.assertEquals(200, post.getResponseCode());
    }

    public void doUploadTSVDataFailure(String testTable, StringBuilder data, int responseCode, Subject subject) throws IOException, PrivilegedActionException {
        URL postURL = new URL(certLoadURL.toString() + "/" + testTable);
        final HttpPost post = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, UTF_8), false);
        Subject.doAs(subject, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                post.run();
                return null;
            }
        });
        Assert.assertEquals(responseCode, post.getResponseCode());
        Assert.assertNotNull(post.getThrowable());
        log.info("response message: " + post.getResponseBody());
    }

    public void verifyAllDataTypes(VOTableTable vt, boolean isParquet, boolean parquetWithoutMetadata) throws IOException, URISyntaxException {
        Iterator<List<Object>> it = vt.getTableData().iterator();
        int count = 0;
        while (it.hasNext()) {
            List<Object> next = it.next();
            Assert.assertEquals("string" + count, next.get(0));
            if (isParquet) {
                Assert.assertEquals((int) Short.MAX_VALUE, next.get(1));
                Assert.assertArrayEquals(new int[]{Short.MIN_VALUE, Short.MAX_VALUE}, (int[]) next.get(11));
            } else {
                Assert.assertEquals(Short.MAX_VALUE, next.get(1));
                Assert.assertArrayEquals(new short[]{Short.MIN_VALUE, Short.MAX_VALUE}, (short[]) next.get(11));
            }
            Assert.assertEquals(Integer.MAX_VALUE, next.get(2));
            Assert.assertEquals(Long.MAX_VALUE, next.get(3));
            Assert.assertEquals(Float.MAX_VALUE, next.get(4));
            Assert.assertEquals(Double.MAX_VALUE, next.get(5));
            Assert.assertTrue(next.get(6) instanceof Date);

            if (parquetWithoutMetadata) {
                Assert.assertArrayEquals(new double[]{1.0, 2.0}, (double[]) next.get(7), 0.0001);
                Assert.assertArrayEquals(new double[]{1.0, 2.0}, (double[]) next.get(8), 0.0001);
                Assert.assertArrayEquals(new double[]{1.0, 2.0, 3.0}, (double[]) next.get(9), 0.0001);
                Assert.assertArrayEquals(new double[]{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}, (double[]) next.get(10), 0.0001);
                Assert.assertEquals("ivo://opencadc.org/youcat", next.get(16));
            } else {
                assertEquals(new DoubleInterval(1.0, 2.0), (DoubleInterval) next.get(7));
                assertEquals(new Point(1.0, 2.0), (Point) next.get(8));
                assertEquals(new Circle(new Point(1, 2), 3), (Circle) next.get(9));
                Polygon p = new Polygon();
                p.getVertices().add(new Point(1.0, 2.0));
                p.getVertices().add(new Point(3.0, 4.0));
                p.getVertices().add(new Point(5.0, 6.0));
                assertEquals(p, (Polygon) next.get(10));
                Assert.assertEquals(new URI("ivo://opencadc.org/youcat"), next.get(16));
            }

            Assert.assertArrayEquals(new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE}, (int[]) next.get(12));
            Assert.assertArrayEquals(new long[]{Long.MIN_VALUE, Long.MAX_VALUE}, (long[]) next.get(13));
            Assert.assertArrayEquals(new float[]{Float.MIN_VALUE, Float.MAX_VALUE}, (float[]) next.get(14), 0.0001f);
            Assert.assertArrayEquals(new double[]{Double.MIN_VALUE, Double.MAX_VALUE}, (double[]) next.get(15), 0.0001);
            Assert.assertTrue(next.get(17) instanceof UUID);
            Assert.assertEquals(STATIC_UUID, next.get(17));
            count++;
        }
        Assert.assertEquals(10, count);
    }

    public String doQuery(String testTable) throws Exception {
        // TAP query check (metadata and actual table exists)
        String adql = "SELECT * from " + testTable;
        Map<String, Object> params = new TreeMap<>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        String result = Subject.doAs(anon, new AuthQueryTest.SyncQueryAction(anonQueryURL, params));
        Assert.assertNotNull(result);
        return result;
    }

    public VOTableTable doQueryForVOT(String testTable) throws Exception {
        String result = doQuery(testTable);
        VOTableReader r = new VOTableReader();
        VOTableDocument doc = r.read(result);
        VOTableResource vr = doc.getResourceByType("results");
        VOTableTable vt = vr.getTable();
        Assert.assertNotNull(vt);
        Assert.assertNotNull(vt.getTableData());
        return vt;
    }

}
