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
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.vosi.InvalidTableSetException;
import ca.nrc.cadc.vosi.TableReader;
import ca.nrc.cadc.vosi.actions.TableDescHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
    public void testCreateQueryDropVOSI() {
        try {
            clearSchemaPerms();
            TapPermissions tp = new TapPermissions(null, true, null, null);
            super.setPerms(schemaOwner, testSchemaName, tp, 200);
            
            String testTable = testSchemaName + ".testCreateQueryDropVOSI";
            final TableDesc orig = doCreateTable(schemaOwner, testTable);
            TableDesc td = doVosiCheck(testTable);
            compare(orig, td);
            
            super.setPerms(schemaOwner, testTable, tp, 200);
            
            VOTableTable vt = doQueryCheck(testTable);
            TableData tdata = vt.getTableData();
            Iterator<List<Object>> iter = tdata.iterator();
            Assert.assertFalse("no result rows", iter.hasNext());
            
            // cleanup on success
            //doDelete(schemaOwner, testTable, false);
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
                    VOTableWriter w = new VOTableWriter(TableDescHandler.VOTABLE_TYPE);
                    w.write(doc, out);
                }
            };
            HttpUpload put = new HttpUpload(src, tableURL);
            put.setContentType(TableDescHandler.VOTABLE_TYPE);
            Subject.doAs(schemaOwner, new RunnableAction(put));
            Assert.assertNull("throwable", put.getThrowable());
            Assert.assertEquals("response code", 200, put.getResponseCode());
            put = null;
            
            TableDesc td = doVosiCheck(testTable);
            
            super.setPerms(schemaOwner, testTable, tp, 200);
            
            VOTableTable vt = doQueryCheck(testTable);
            TableData tdata = vt.getTableData();
            Iterator<List<Object>> iter = tdata.iterator();
            Assert.assertFalse("no result rows", iter.hasNext());
            
            
            //doDelete(schemaOwner, testTable, false);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
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
