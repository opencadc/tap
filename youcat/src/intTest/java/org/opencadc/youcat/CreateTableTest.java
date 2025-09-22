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
import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.tables.ListTableData;
import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.net.*;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vosi.TableSetWriter;
import ca.nrc.cadc.vosi.TableWriter;
import ca.nrc.cadc.vosi.actions.TablesInputHandler;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.PrivilegedActionException;
import java.util.*;
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
        testCreateQueryDropVOTable(true);
        testCreateQueryDropVOTable(false);
    }

    public void testCreateQueryDropVOTable(boolean uploadTableData) {
        try {
            clearSchemaPerms();
            TapPermissions tp = new TapPermissions(null, true, null, null);
            super.setPerms(schemaOwner, testSchemaName, tp, 200);
            
            String testTable = testSchemaName + ".testCreateQueryDropVOTable";

            // cleanup just in case
            doDelete(schemaOwner, testTable, true);

            VOTableTable actualVOTableTable = prepareVOTableTable(uploadTableData);
            createTableFromVOTable(actualVOTableTable, testTable);

            TableDesc td = doVosiCheck(testTable);
            
            super.setPerms(schemaOwner, testTable, tp, 200);
            
            VOTableTable vt = doQueryCheck(testTable);
            VOTableField field0 = vt.getFields().get(0);
            Assert.assertNull("field ID attr ignored by create", field0.id);
            TableData tdata = vt.getTableData();
            Iterator<List<Object>> iter = tdata.iterator();
            Assert.assertEquals("Table rows are not as per expectation.", uploadTableData, iter.hasNext());

            if (uploadTableData) {
                verifyUploadedVOTableTableData(iter, actualVOTableTable.getTableData().iterator());
            }

            // cleanup on success
            doDelete(schemaOwner, testTable, false);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCreateQueryDropParquet() throws Exception {
        String testTable = testSchemaName + ".testCreateQueryDropParquet";

        // Permission updates
        clearSchemaPerms();
        TapPermissions tp = new TapPermissions(null, true, null, null);
        super.setPerms(schemaOwner, testSchemaName, tp, 200);

        // delete table if it exists
        doDelete(schemaOwner, testTable, true);

        // Create table from VOTable data
        VOTableTable actualVOTableTable = prepareVOTableTable(true);
        VOTableDocument voTableDocument = createTableFromVOTable(actualVOTableTable, testTable);

        // Create table from Parquet data
        createTableFromParquet(testTable);

        super.setPerms(schemaOwner, testTable, tp, 200);

        // Verify the table created from Parquet data
        VOTableTable voTableTable = doQueryCheck(testTable);
        TableData tdata = voTableTable.getTableData();
        Assert.assertEquals(voTableDocument.getResourceByType("results").getTable().getFields().size(), voTableTable.getFields().size());

        Iterator<List<Object>> iter = tdata.iterator();
        Assert.assertTrue("no result rows", iter.hasNext());

        verifyUploadedVOTableTableData(iter, actualVOTableTable.getTableData().iterator());
    }

    /*
     * Steps:
     * - get the data from testTable in Parquet format
     * - delete the testTable table
     * - create a new testTable table using Parquet format
     * */
    private void createTableFromParquet(String testTable) throws Exception {
        // get data in parquet format
        ByteArrayOutputStream parquetData = getParquetData(testTable);

        doDelete(schemaOwner, testTable, false);

        // Create table using Parquet data
        URL tableURL = new URL(certTablesURL.toExternalForm() + "/" + testTable);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(parquetData.toByteArray());

        HttpUpload put = new HttpUpload(inputStream, tableURL);
        put.setRequestProperty(HttpTransfer.CONTENT_TYPE, TablesInputHandler.PARQUET_TYPE);
        Subject.doAs(schemaOwner, new RunnableAction(put));
        Assert.assertNull("throwable", put.getThrowable());
        Assert.assertEquals("response code", 200, put.getResponseCode());
    }

    private ByteArrayOutputStream getParquetData(String testTable) throws PrivilegedActionException {
        String adql = "SELECT * from " + testTable;

        Map<String, Object> params = new TreeMap<>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        params.put("RESPONSEFORMAT", "parquet");

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String result = Subject.doAs(schemaOwner, new AuthQueryTest.SyncQueryAction(anonQueryURL, params, out, "application/vnd.apache.parquet"));
        Assert.assertNotNull(result);

        return out;
    }

    private static VOTableTable prepareVOTableTable(boolean testUploadTableData) {
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

        if (testUploadTableData) {
            addTableDataToVOTable(vtab);
        }

        return vtab;
    }

    private static void addTableDataToVOTable(VOTableTable vtab) {
        ListTableData tableData = new ListTableData();
        vtab.setTableData(tableData);

        // prepare iterator with dummy data
        for(int i=0; i<10; i++) {
            List<Object> row = new ArrayList<>();
            row.add("string" + i); // c0
            row.add(Short.MAX_VALUE); // c1
            row.add(Integer.MAX_VALUE); // c2
            row.add(Long.MAX_VALUE); // c3
            row.add(Float.MAX_VALUE); // c4
            row.add(Double.MAX_VALUE); // c5

            row.add(new Date()); // e6
            row.add(new Interval(1.0, 2.0)); // e7
            row.add(new Point(1.0, 2.0)); // e8
            row.add(new Circle(new Point(1, 2), 3)); // e9

            Polygon p = new Polygon();
            p.getVertices().add(new Point(1.0, 2.0));
            p.getVertices().add(new Point(3.0, 4.0));
            p.getVertices().add(new Point(5.0, 6.0));
            row.add(p); // e10

            row.add(new short[] { (short) i, (short) (i + 1) }); // a11
            row.add(new int[] { i, i + 1 }); // a12
            row.add(new long[] { i, i + 1 }); // a13
            row.add(new float[] { i, i + 1 }); // a14
            row.add(new double[] { i, i + 1 }); // a15
            tableData.getArrayList().add(row);
        }
    }

    private VOTableDocument createTableFromVOTable(VOTableTable vtab, String testTable) throws MalformedURLException {
        VOTableResource vres = new VOTableResource("results");
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

        return doc;
    }

    private void verifyUploadedVOTableTableData(Iterator<List<Object>> retrievedDataIter, Iterator<List<Object>> actualDataIter) {
        int count = 0;
        while (retrievedDataIter.hasNext() && actualDataIter.hasNext()) {
            List<Object> retrievedRow = retrievedDataIter.next();
            List<Object> actualRow = actualDataIter.next();
            Assert.assertEquals("string" + count, retrievedRow.get(0));
            Assert.assertEquals(actualRow.get(2), retrievedRow.get(2));
            Assert.assertEquals(actualRow.get(3), retrievedRow.get(3));
            Assert.assertEquals(actualRow.get(4), retrievedRow.get(4));
            Assert.assertEquals(actualRow.get(5), retrievedRow.get(5));
            Assert.assertTrue(retrievedRow.get(6) instanceof Date);

            Assert.assertEquals(actualRow.get(7), retrievedRow.get(7));
            assertEquals((Point) actualRow.get(8), (Point) retrievedRow.get(8));
            assertEquals((Circle) actualRow.get(9), (Circle) retrievedRow.get(9));
            Polygon p = new Polygon();
            p.getVertices().add(new Point(1.0, 2.0));
            p.getVertices().add(new Point(3.0, 4.0));
            p.getVertices().add(new Point(5.0, 6.0));
            assertEquals(p, (Polygon) retrievedRow.get(10));

            count++;
        }
        Assert.assertEquals(10, count);
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

}
