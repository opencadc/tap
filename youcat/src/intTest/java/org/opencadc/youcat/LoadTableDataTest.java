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

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.tap.schema.TapPermissions;
import ca.nrc.cadc.vosi.actions.TableContentHandler;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import uk.ac.starlink.fits.FitsTableWriter;
import uk.ac.starlink.table.StarTable;
import uk.ac.starlink.table.StarTableOutput;
import uk.ac.starlink.table.StoragePolicy;
import uk.ac.starlink.util.DataSource;
import uk.ac.starlink.votable.VOTableBuilder;

/**
 *
 * @author majorb
 */
public class LoadTableDataTest extends AbstractTablesTest {
    private static final Logger log = Logger.getLogger(LoadTableDataTest.class);
    
    private static final Charset UTF8 = Charset.forName("utf-8");
    
    
    public LoadTableDataTest() { 
        super();
    }
    
    private String doQuery(String testTable) throws Exception {
        // TAP query check (metadata and actual table exists)
        String adql = "SELECT * from " + testTable;
        Map<String, Object> params = new TreeMap<String, Object>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        String result = Subject.doAs(anon, new AuthQueryTest.SyncQueryAction(anonQueryURL, params));
        Assert.assertNotNull(result);
        return result;
    }
    
    private VOTableTable doQueryForVOT(String testTable) throws Exception {
        String result = doQuery(testTable);
        VOTableReader r = new VOTableReader();
        VOTableDocument doc = r.read(result);
        VOTableResource vr = doc.getResourceByType("results");
        VOTableTable vt = vr.getTable();
        Assert.assertNotNull(vt);
        Assert.assertNotNull(vt.getTableData());
        return vt;
    }
    
    @Test
    public void testPostNoTableName() {
        try {
            log.info("start");
            
            String testTable = testSchemaName + ".testPostNoTableName";
            doCreateTable(schemaOwner, testTable);
            
            StringBuilder data = new StringBuilder();
            data.append("c0\n");
            data.append("string\n");
            
            URL postURL = new URL(certLoadURL.toString());
            final HttpPost post = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, UTF8), false);
            Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post.run();
                    return null;
                }
            });
            Assert.assertEquals(400, post.getResponseCode());
            Assert.assertNotNull(post.getThrowable());
            
            doDelete(schemaOwner, testTable, false);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPostInvalidColumnName() {
        try {
            log.info("start");
            
            String testTable = testSchemaName + ".testPostInvalidColumnName";
            doCreateTable(schemaOwner, testTable);
            
            StringBuilder data = new StringBuilder();
            data.append("string\n");
            data.append("string\n");
            
            URL postURL = new URL(certLoadURL.toString() + "/" + testTable);
            final HttpPost post = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, UTF8), false);
            Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post.run();
                    return null;
                }
            });
            Assert.assertEquals(400, post.getResponseCode());
            Assert.assertNotNull(post.getThrowable());
            
            doDelete(schemaOwner, testTable, false);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testWrongNumberOfColumns() {
        try {
            log.info("start");
            
            String testTable = testSchemaName + ".testWrongNumberOfColumns";
            doCreateTable(schemaOwner, testTable);
            
            StringBuilder data = new StringBuilder();
            data.append("c0\tc1\n");
            data.append("string");
            
            URL postURL = new URL(certLoadURL.toString() + "/" + testTable);
            final HttpPost post = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, UTF8), false);
            Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post.run();
                    return null;
                }
            });
            Assert.assertEquals(400, post.getResponseCode());
            Assert.assertNotNull(post.getThrowable());
            
            doDelete(schemaOwner, testTable, false);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testNoSuchTable() {
        try {
            log.info("start");
            
            StringBuilder data = new StringBuilder();
            data.append("c0\tc1\n");
            data.append("string");
            
            URL postURL = new URL(certLoadURL.toString() + "/" + testSchemaName + ".noSuchTable");
            final HttpPost post = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, UTF8), false);
            Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post.run();
                    return null;
                }
            });
            log.info(post.getThrowable());
            Assert.assertEquals(404, post.getResponseCode());
            Assert.assertNotNull(post.getThrowable());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testInvalidTableName() {
        try {
            log.info("start");
            
            StringBuilder data = new StringBuilder();
            data.append("c0\tc1\n");
            data.append("string");
            
            URL postURL = new URL(certLoadURL.toString() + "/" + testSchemaName + ".invalid.table.name");
            final HttpPost post = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, UTF8), false);
            Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post.run();
                    return null;
                }
            });
            Assert.assertEquals(404, post.getResponseCode());
            Assert.assertNotNull(post.getThrowable());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testNotTableOwner() {
        try {
            log.info("start");
            
            clearSchemaPerms();
            
            String testTable = testSchemaName + ".testNotTableOwner";
            doCreateTable(schemaOwner, testTable);
            
            StringBuilder data = new StringBuilder();
            data.append("c0\tc1\n");
            data.append("string");
            
            URL postURL = new URL(certLoadURL.toString() + "/" + testTable);
            final HttpPost post = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, UTF8), false);
            Subject.doAs(subjectWithGroups, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post.run();
                    return null;
                }
            });
            Assert.assertEquals(403, post.getResponseCode());
            Assert.assertNotNull(post.getThrowable());
            
            doDelete(schemaOwner, testTable, false);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testAllDataTypesTSV() {
        try {
            log.info("start");
            
            TapPermissions tp = new TapPermissions(null, true, null, null);
            setPerms(schemaOwner, testSchemaName, tp, 200);
            
            String testTable = testSchemaName + ".testAllDataTypesTSV";
            doCreateTable(schemaOwner, testTable);
            setPerms(schemaOwner, testTable, tp, 200);
            
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
            
            URL postURL = new URL(certLoadURL.toString() + "/" + testTable);
            final HttpPost post = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, UTF8), false);
            Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post.run();
                    return null;
                }
            });
            Assert.assertNull(post.getThrowable());
            Assert.assertEquals(200, post.getResponseCode());
            
            VOTableTable vt = doQueryForVOT(testTable);
            Iterator<List<Object>> it = vt.getTableData().iterator();
            int count = 0;
            while (it.hasNext()) {
                List<Object> next = it.next();
                Assert.assertEquals("string" + count, (String) next.get(0));
                Assert.assertEquals(new Short(Short.MAX_VALUE), (Short) next.get(1));
                Assert.assertEquals(new Integer(Integer.MAX_VALUE), (Integer) next.get(2));
                Assert.assertEquals(new Long(Long.MAX_VALUE), (Long) next.get(3));
                Assert.assertEquals(new Float(Float.MAX_VALUE), (Float) next.get(4));
                Assert.assertEquals(new Double(Double.MAX_VALUE), (Double) next.get(5));
                Assert.assertTrue(next.get(6) instanceof Date);
                assertEquals(new DoubleInterval(1.0, 2.0), (DoubleInterval) next.get(7)); 
                assertEquals(new Point(1.0, 2.0), (Point) next.get(8));
                assertEquals(new Circle(new Point(1, 2), 3), (Circle) next.get(9));
                Polygon p = new Polygon();
                p.getVertices().add(new Point(1.0, 2.0));
                p.getVertices().add(new Point(3.0, 4.0));
                p.getVertices().add(new Point(5.0, 6.0));
                assertEquals(p, (Polygon) next.get(10));
                count++;
            }
            Assert.assertEquals(10, count);
            
            doDelete(schemaOwner, testTable, false);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testAllDataTypesFITS() {
        try {
            log.info("start");
            
            TapPermissions tp = new TapPermissions(null, true, null, null);
            setPerms(schemaOwner, testSchemaName, tp, 200);
            
            String testTable = testSchemaName + ".testAllDataTypesFits";
            doCreateTable(schemaOwner, testTable);
            setPerms(schemaOwner, testTable, tp, 200);
            
            StringBuilder data = new StringBuilder();
            data.append("c0\tc1\tc2\tc3\tc4\tc5\tc6\te7\te8\te9\te10\n");
            //data.append("c0\tc1\tc2\tc3\tc4\tc5\n");
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
            
            URL postURL = new URL(certLoadURL.toString() + "/" + testTable);
            final HttpPost post = new HttpPost(postURL, data.toString(), TableContentHandler.CONTENT_TYPE_TSV, false);
            Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post.run();
                    return null;
                }
            });
            Assert.assertNull(post.getThrowable());
            Assert.assertEquals(200, post.getResponseCode());
            
            final String voTableString = doQuery(testTable);
            
            log.info("VOTable table: " + voTableString);
            
            // convert to fits table
            DataSource ds = new DataSource() {
                protected InputStream getRawInputStream() throws IOException {
                    return new ByteArrayInputStream(voTableString.getBytes());
                }
            };
            VOTableBuilder voTableBuilder = new VOTableBuilder();
            StarTable st = voTableBuilder.makeStarTable(ds, false, StoragePolicy.PREFER_MEMORY);
            StarTableOutput tableOutput = new StarTableOutput();
            String fitsFilename = "build/tmp/" + testTable + ".fits";
            File fitsFile = new File(fitsFilename);
            FileOutputStream out = new FileOutputStream(fitsFile);
            tableOutput.writeStarTable(st, out, new FitsTableWriter());
            out.flush();
            
            doDelete(schemaOwner, testTable, false);
            doCreateTable(schemaOwner, testTable);
            setPerms(schemaOwner, testTable, tp, 200);
            
            // Post the FITS table
            byte[] bytes = Files.readAllBytes(Paths.get(fitsFilename));
            FileContent fileContent = new FileContent(bytes, "application/fits");
            final HttpPost post2 = new HttpPost(postURL, fileContent, false);
            Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post2.run();
                    return null;
                }
            });
            Assert.assertNull(post2.getThrowable());
            Assert.assertEquals(200, post2.getResponseCode());
            
            // next: query the table and assert results are correct
            VOTableTable vt = doQueryForVOT(testTable);
            Iterator<List<Object>> it = vt.getTableData().iterator();
            int count = 0;
            while (it.hasNext()) {
                List<Object> next = it.next();
                Assert.assertEquals("string" + count, (String) next.get(0));
                Assert.assertEquals(new Short(Short.MAX_VALUE), (Short) next.get(1));
                Assert.assertEquals(new Integer(Integer.MAX_VALUE), (Integer) next.get(2));
                Assert.assertEquals(new Long(Long.MAX_VALUE), (Long) next.get(3));
                Assert.assertEquals(new Float(Float.MAX_VALUE), (Float) next.get(4));
                Assert.assertEquals(new Double(Double.MAX_VALUE), (Double) next.get(5));
                Assert.assertTrue(next.get(6) instanceof Date);
                assertEquals(new DoubleInterval(1.0, 2.0), (DoubleInterval) next.get(7)); 
                assertEquals(new Point(1.0, 2.0), (Point) next.get(8));
                assertEquals(new Circle(new Point(1, 2), 3), (Circle) next.get(9));
                Polygon p = new Polygon();
                p.getVertices().add(new Point(1.0, 2.0));
                p.getVertices().add(new Point(3.0, 4.0));
                p.getVertices().add(new Point(5.0, 6.0));
                assertEquals(p, (Polygon) next.get(10));
                count++;
            }
            Assert.assertEquals(10, count);
            
            doDelete(schemaOwner, testTable, false);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testMixedContentTypeASCII() {
        try {
            log.info("start");
            
            TapPermissions tp = new TapPermissions(null, true, null, null);
            setPerms(schemaOwner, testSchemaName, tp, 200);
            
            String testTable = testSchemaName + ".testMixedContentTypeASCII";
            doCreateTable(schemaOwner, testTable);
            setPerms(schemaOwner, testTable, tp, 200);
            
            StringBuilder data = new StringBuilder();
            data.append("c0, c6, c2\n");
            for (int i=0; i<10; i++) {
                data.append("string" + i + ",2018-11-05T22:12:33.111," + i + "\n");
            }
            
            URL postURL = new URL(certLoadURL.toString() + "/" + testTable);
            final HttpPost post1 = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_CSV, UTF8), false);
            Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post1.run();
                    return null;
                }
            });
            Assert.assertNull(post1.getThrowable());
            Assert.assertEquals(200, post1.getResponseCode());
            
            VOTableTable vt = doQueryForVOT(testTable);
            Iterator<List<Object>> it = vt.getTableData().iterator();
            int count = 0;
            while (it.hasNext()) {
                List<Object> next = it.next();
                Assert.assertEquals("string" + count, (String) next.get(0)); 
                Assert.assertTrue(next.get(6) instanceof Date);
                Assert.assertEquals(count, ((Integer) next.get(2)).intValue());
                count++;
            }
            Assert.assertEquals(10, count);
            
            data = new StringBuilder();
            data.append("c0\tc6\tc2\n");
            for (int i=10; i<20; i++) {
                data.append("string" + i + "\t2018-11-05T22:12:33.111\t" + i + "\n");
            }
            
            final HttpPost post2 = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, UTF8), false);
            Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post2.run();
                    return null;
                }
            });
            Assert.assertNull(post2.getThrowable());
            Assert.assertEquals(200, post2.getResponseCode());
            
            vt = doQueryForVOT(testTable);
            it = vt.getTableData().iterator();
            count = 0;
            while (it.hasNext()) {
                List<Object> next = it.next();
                Assert.assertEquals("string" + count, (String) next.get(0)); 
                Assert.assertTrue(next.get(6) instanceof Date);
                Assert.assertEquals(count, ((Integer) next.get(2)).intValue());
                count++;
            }
            Assert.assertEquals(20, count);
            
            doDelete(schemaOwner, testTable, false);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testMultipleBatches() {
        try {
            log.info("start");
            
            TapPermissions tp = new TapPermissions(null, true, null, null);
            setPerms(schemaOwner, testSchemaName, tp, 200);
            
            String testTable = testSchemaName + ".testMultipleBatches";
            doCreateTable(schemaOwner, testTable);
            setPerms(schemaOwner, testTable, tp, 200);
            
            StringBuilder data = new StringBuilder();
            data.append("c0\tc6\tc2\n");
            for (int i=0; i<3500; i++) {
                data.append("string" + i + "\t2018-11-05T22:12:33.111\t" + i + "\n");
            }
            
            URL postURL = new URL(certLoadURL.toString() + "/" + testTable);
            final HttpPost post = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, UTF8), false);
            Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post.run();
                    return null;
                }
            });
            Assert.assertNull(post.getThrowable());
            Assert.assertEquals(200, post.getResponseCode());
            
            VOTableTable vt = doQueryForVOT(testTable);
            Iterator<List<Object>> it = vt.getTableData().iterator();
            int count = 0;
            while (it.hasNext()) {
                List<Object> next = it.next();
                Assert.assertEquals("string" + count, (String) next.get(0)); 
                Assert.assertTrue(next.get(6) instanceof Date);
                Assert.assertEquals(count, ((Integer) next.get(2)).intValue());
                count++;
            }
            Assert.assertEquals(3500, count);
            
            doDelete(schemaOwner, testTable, false);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testErrorInMiddle() {
        try {
            log.info("start");
            
            TapPermissions tp = new TapPermissions(null, true, null, null);
            setPerms(schemaOwner, testSchemaName, tp, 200);
            
            String testTable = testSchemaName + ".testErrorInMiddle";
            doCreateTable(schemaOwner, testTable);
            setPerms(schemaOwner, testTable, tp, 200);
            
            StringBuilder data = new StringBuilder();
            data.append("c0\tc6\tc2\n");
            for (int i=0; i<1100; i++) {
                data.append("string" + i + "\t2018-11-05T22:12:33.111\t" + i + "\n");
            }
            // add in the middle a single row that has the 'int' column set to the letter 'a'.
            data.append("string1101\t2018-11-05T22:12:33.111\ta\n");
            for (int i=1101; i<1200; i++) {
                data.append("string" + i + "\t2018-11-05T22:12:33.111\t" + i + "\n");
            }
            
            URL postURL = new URL(certLoadURL.toString() + "/" + testTable);
            final HttpPost post = new HttpPost(postURL, new FileContent(data.toString(), TableContentHandler.CONTENT_TYPE_TSV, UTF8), false);
            Subject.doAs(schemaOwner, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    post.run();
                    return null;
                }
            });
            Assert.assertNotNull(post.getThrowable());
            Assert.assertEquals(400, post.getResponseCode());
            log.info("response message: " + post.getResponseBody());
            
            // make sure the first 1000 (batch size) got in
            VOTableTable vt = doQueryForVOT(testTable);
            Iterator<List<Object>> it = vt.getTableData().iterator();
            int count = 0;
            while (it.hasNext()) {
                List<Object> next = it.next();
                Assert.assertEquals("string" + count, (String) next.get(0)); 
                Assert.assertTrue(next.get(6) instanceof Date);
                Assert.assertEquals(count, ((Integer) next.get(2)).intValue());
                count++;
            }
            Assert.assertEquals(1000, count);
            log.info("Count: " + count);
            
            doDelete(schemaOwner, testTable, false);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
}
