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

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vosi.InvalidTableSetException;
import ca.nrc.cadc.vosi.TableReader;
import ca.nrc.cadc.vosi.TableSetReader;
import ca.nrc.cadc.vosi.TableSetWriter;
import ca.nrc.cadc.vosi.TableWriter;
import ca.nrc.cadc.vosi.actions.TablesInputHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class CreateTableTest extends AbstractTablesTest {
    private static final Logger log = Logger.getLogger(CreateTableTest.class);
    
    static {
        Log4jInit.setLevel("org.opencadc.youcat", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
    }

    public CreateTableTest() { 
        super();
    }
    
    private static class StreamTableReader implements InputStreamWrapper {
        TableDesc td;
        
        @Override
        public void read(InputStream in) throws IOException {
            try {
                TableReader r = new  TableReader(false); // schema validation causes default arraysize="1" to be injected
                td = r.read(in);
            } catch (InvalidTableSetException ex) {
                throw new RuntimeException("invalid table metadata: ", ex);
            }
        }
    }
    
    private static class StreamSchemaReader implements InputStreamWrapper {
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

    private SchemaDesc doVosiSchemaCheck(Subject caller, String schemaName) throws Exception {
        // VOSI tables check (metadata)
        URL getTableURL = new URL(anonTablesURL.toExternalForm() + "/" + schemaName);
        StreamSchemaReader isw = new StreamSchemaReader();
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

    private TableDesc doVosiCheck(String testTable) throws Exception {
        // VOSI tables check (metadata)
        URL getTableURL = new URL(anonTablesURL.toExternalForm() + "/" + testTable);
        StreamTableReader isw = new StreamTableReader();
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
    
    private VOTableTable doQueryCheck(String testTable) throws Exception {
        // TAP query check (metadata and actual table exists)
        String adql = "SELECT * from " + testTable;
        Map<String, Object> params = new TreeMap<String, Object>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        log.info("doQueryCheck: " + testTable + " " + anonQueryURL);
        String result = Subject.doAs(anon, new AuthQueryTest.SyncQueryAction(anonQueryURL, params));
        log.info("doQueryCheck: verifying...");
        Assert.assertNotNull(result);
        VOTableReader r = new VOTableReader();
        VOTableDocument doc = r.read(result);
        VOTableResource vr = doc.getResourceByType("results");
        VOTableTable vt = vr.getTable();
        Assert.assertNotNull(vt);
        return vt;
    }
    
    @Test
    public void testCreateQueryUpdateDropVOSI() {
        try {
            clearSchemaPerms();
            TapPermissions tp = new TapPermissions(null, true, null, null);
            super.setPerms(schemaOwner, testSchemaName, tp, 200);
            
            String testTable = testSchemaName + ".testCreateQueryUpdateDropVOSI";
            final TableDesc orig = doCreateTable(schemaOwner, testTable);
            TableDesc td = doVosiCheck(testTable);
            compare(orig, td);
            
            super.setPerms(schemaOwner, testTable, tp, 200);
            
            VOTableTable vt = doQueryCheck(testTable);
            TableData tdata = vt.getTableData();
            Iterator<List<Object>> iter = tdata.iterator();
            Assert.assertFalse("no result rows", iter.hasNext());
            
            // modify table description
            td.description = "updated by IntTest";
            td.utype = "namespace:MyCustomModel";
            final ColumnDesc col0 = td.getColumnDescs().get(0);
            col0.description = "primary key";
            //col0.principal = true;
            //col0.std = true;
            col0.columnID = "pk";
            
            final URL tableURL = new URL(certTablesURL.toExternalForm() + "/" + testTable);
            TableWriter w = new TableWriter();
            StringWriter sw = new StringWriter();
            w.write(td, sw);
            String xml = sw.toString();
            log.info("updating...\n" + xml);
            FileContent fc = new FileContent(xml, TablesInputHandler.VOSI_TABLE_TYPE, Charset.forName("UTF-8"));
            HttpPost update = new HttpPost(tableURL, fc, false);
            Subject.doAs(schemaOwner, new RunnableAction(update));
            log.info("update: " + update.getResponseCode() + " " + update.getThrowable());
            Assert.assertEquals(204, update.getResponseCode());
            
            TableDesc td2 = doVosiCheck(testTable);
            compare(td, td2);
            
            final ColumnDesc c0 = td.getColumnDescs().get(0);
            
            log.info("illegal update: change column data type");
            c0.getDatatype().xtype = "custom:thing";
            sw = new StringWriter();
            w.write(td, sw);
            xml = sw.toString();
            log.debug("illegal update:\n" + xml);
            fc = new FileContent(xml, TablesInputHandler.VOSI_TABLE_TYPE, Charset.forName("UTF-8"));
            update = new HttpPost(tableURL, fc, false);
            Subject.doAs(schemaOwner, new RunnableAction(update));
            log.info("update: " + update.getResponseCode() + " " + update.getThrowable());
            Assert.assertEquals(400, update.getResponseCode());
            Assert.assertTrue(update.getThrowable().getMessage().contains("TapDataType"));
            c0.getDatatype().xtype = null;
                    
            log.info("illegal update: add column");
            ColumnDesc ecd = new ColumnDesc(td.getTableName(), "add-by-update", TapDataType.CHAR);
            td.getColumnDescs().add(ecd);
            sw = new StringWriter();
            w.write(td, sw);
            xml = sw.toString();
            log.debug("illegal update:\n" + xml);
            fc = new FileContent(xml, TablesInputHandler.VOSI_TABLE_TYPE, Charset.forName("UTF-8"));
            update = new HttpPost(tableURL, fc, false);
            Subject.doAs(schemaOwner, new RunnableAction(update));
            log.info("update: " + update.getResponseCode() + " " + update.getThrowable());
            Assert.assertEquals(400, update.getResponseCode());
            Assert.assertTrue(update.getThrowable().getMessage().contains("cannot add/remove/rename"));
            td.getColumnDescs().remove(ecd);
            
            log.info("illegal update: remove column");
            td.getColumnDescs().remove(c0);
            sw = new StringWriter();
            w.write(td, sw);
            xml = sw.toString();
            log.debug("illegal update:\n" + xml);
            fc = new FileContent(xml, TablesInputHandler.VOSI_TABLE_TYPE, Charset.forName("UTF-8"));
            update = new HttpPost(tableURL, fc, false);
            Subject.doAs(schemaOwner, new RunnableAction(update));
            log.info("update: " + update.getResponseCode() + " " + update.getThrowable());
            Assert.assertEquals(400, update.getResponseCode());
            Assert.assertTrue(update.getThrowable().getMessage().contains("cannot add/remove/rename"));
            
            log.info("illegal update: rename column");
            ColumnDesc rename = new ColumnDesc(td.getTableName(), c0.getColumnName() + "-renamed", c0.getDatatype());
            td.getColumnDescs().remove(c0);
            td.getColumnDescs().add(0, rename);
            sw = new StringWriter();
            w.write(td, sw);
            xml = sw.toString();
            log.debug("illegal update:\n" + xml);
            fc = new FileContent(xml, TablesInputHandler.VOSI_TABLE_TYPE, Charset.forName("UTF-8"));
            update = new HttpPost(tableURL, fc, false);
            Subject.doAs(schemaOwner, new RunnableAction(update));
            log.info("update: " + update.getResponseCode() + " " + update.getThrowable());
            Assert.assertEquals(400, update.getResponseCode());
            Assert.assertTrue(update.getThrowable().getMessage().contains("cannot add/remove/rename"));
            td.getColumnDescs().remove(rename);
            td.getColumnDescs().add(0, c0);
            
            // cleanup on success
            doDelete(schemaOwner, testTable, false);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCreateQueryDropVOTable() {
        try {
            clearSchemaPerms();
            TapPermissions tp = new TapPermissions(null, true, null, null);
            super.setPerms(schemaOwner, testSchemaName, tp, 200);
            
            String testTable = testSchemaName + ".testCreateQueryDropVOTable";

            // cleanup just in case
            doDelete(schemaOwner, testTable, true);

            VOTableTable vtab = new VOTableTable();
            vtab.getFields().add(new VOTableField("c0", TapDataType.STRING.getDatatype(), TapDataType.STRING.arraysize));
            vtab.getFields().add(new VOTableField("c1", TapDataType.SHORT.getDatatype()));
            vtab.getFields().add(new VOTableField("c2", TapDataType.INTEGER.getDatatype()));
            vtab.getFields().add(new VOTableField("c3", TapDataType.LONG.getDatatype()));
            vtab.getFields().add(new VOTableField("c4", TapDataType.FLOAT.getDatatype()));
            vtab.getFields().add(new VOTableField("c5", TapDataType.DOUBLE.getDatatype()));
            VOTableField f0 = vtab.getFields().get(0);
            f0.id = "bogus_id";
            
            // extended types
            VOTableField tf;
            tf = new VOTableField("e6", TapDataType.TIMESTAMP.getDatatype(), TapDataType.TIMESTAMP.arraysize);
            tf.xtype = TapDataType.TIMESTAMP.xtype;
            vtab.getFields().add(tf);
            
            tf = new VOTableField("e7", TapDataType.INTERVAL.getDatatype(), TapDataType.INTERVAL.arraysize);
            tf.xtype = TapDataType.INTERVAL.xtype;
            vtab.getFields().add(tf);
            tf = new VOTableField("e8", TapDataType.POINT.getDatatype(), TapDataType.POINT.arraysize);
            tf.xtype = TapDataType.POINT.xtype;
            vtab.getFields().add(tf);
            tf = new VOTableField("e9", TapDataType.CIRCLE.getDatatype(), TapDataType.CIRCLE.arraysize);
            tf.xtype = TapDataType.CIRCLE.xtype;
            vtab.getFields().add(tf);
            tf = new VOTableField("e10", TapDataType.POLYGON.getDatatype(), TapDataType.POLYGON.arraysize);
            tf.xtype = TapDataType.POLYGON.xtype;
            vtab.getFields().add(tf);
            
            // arrays
            vtab.getFields().add(new VOTableField("a11", "short", "*"));
            vtab.getFields().add(new VOTableField("a12", "int", "*"));
            vtab.getFields().add(new VOTableField("a13", "long", "*"));
            vtab.getFields().add(new VOTableField("a14", "float", "*")); 
            vtab.getFields().add(new VOTableField("a15", "double", "*"));
            
            VOTableResource vres = new VOTableResource("meta");
            vres.setTable(vtab);
            final VOTableDocument doc = new VOTableDocument();
            doc.getResources().add(vres);
            
            // create
            URL tableURL = new URL(certTablesURL.toExternalForm() + "/" + testTable);
            OutputStreamWrapper src = new OutputStreamWrapper() {
                @Override
                public void write(OutputStream out) throws IOException {
                    VOTableWriter w = new VOTableWriter(TablesInputHandler.VOTABLE_TYPE);
                    w.write(doc, out);
                }
            };
            HttpUpload put = new HttpUpload(src, tableURL);
            put.setContentType(TablesInputHandler.VOTABLE_TYPE);
            Subject.doAs(schemaOwner, new RunnableAction(put));
            Assert.assertNull("throwable", put.getThrowable());
            Assert.assertEquals("response code", 200, put.getResponseCode());
            put = null;
            
            TableDesc td = doVosiCheck(testTable);
            
            super.setPerms(schemaOwner, testTable, tp, 200);
            
            VOTableTable vt = doQueryCheck(testTable);
            VOTableField field0 = vt.getFields().get(0);
            Assert.assertNull("field ID attr ignored by create", field0.id);
            TableData tdata = vt.getTableData();
            Iterator<List<Object>> iter = tdata.iterator();
            Assert.assertFalse("no result rows", iter.hasNext());
            
            // cleanup on success
            doDelete(schemaOwner, testTable, false);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCreateUpdateDropSchema() {
        
        // TODO: use schemaOwner subject to determine the user name here
        final String owner = "cadcauthtest1";
        
        try {
            final URL schemaURL = new URL(certTablesURL.toExternalForm() + "/" + testCreateSchema);
            
            doDelete(admin, testCreateSchema, true);

            SchemaDesc orig = new SchemaDesc(testCreateSchema);
            orig.description = "original description";
            TableSetWriter w = new TableSetWriter();
            StringWriter sw = new StringWriter();
            TapSchema ts = new TapSchema();
            ts.getSchemaDescs().add(orig);
            w.write(ts, sw);
            String xml = sw.toString();
            log.info("update description:\n" + xml);
            FileContent fc = new FileContent(xml, TablesInputHandler.VOSI_SCHEMA_TYPE, Charset.forName("UTF-8"));
            HttpUpload create = new HttpUpload(fc, schemaURL);
            create.setRequestProperty("x-schema-owner", owner);
            Subject.doAs(admin, new RunnableAction(create));
            log.info("update: " + create.getResponseCode() + " " + create.getThrowable());
            Assert.assertEquals(200, create.getResponseCode());
            
            SchemaDesc sd = doVosiSchemaCheck(schemaOwner, testCreateSchema);
            Assert.assertNotNull(sd);
            Assert.assertEquals(orig.getSchemaName(), sd.getSchemaName());
            Assert.assertEquals(orig.description, sd.description);
            Assert.assertEquals(orig.utype, sd.utype);
            
            log.info("update schema description and utype");
            sd.description = "updated description " + System.currentTimeMillis();
            sd.utype = "custom:data-model";
            w = new TableSetWriter();
            sw = new StringWriter();
            ts = new TapSchema();
            ts.getSchemaDescs().clear();
            ts.getSchemaDescs().add(sd);
            w.write(ts, sw);
            xml = sw.toString();
            log.info("update description:\n" + xml);
            fc = new FileContent(xml, TablesInputHandler.VOSI_SCHEMA_TYPE, Charset.forName("UTF-8"));
            HttpPost update = new HttpPost(schemaURL, fc, false);
            Subject.doAs(schemaOwner, new RunnableAction(update));
            log.info("update: " + update.getResponseCode() + " " + update.getThrowable());
            Assert.assertEquals(204, update.getResponseCode());
            
            SchemaDesc sd2 = doVosiSchemaCheck(schemaOwner, testCreateSchema);
            Assert.assertEquals(sd.description, sd2.description);
            Assert.assertEquals(sd.utype, sd2.utype);
            
            // cleanup on success
            doDelete(admin, testCreateSchema, false);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private void compare(TableDesc expected, TableDesc actual) {
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
    
    private void compare(VOTableTable expected, VOTableTable actual) {
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
}
