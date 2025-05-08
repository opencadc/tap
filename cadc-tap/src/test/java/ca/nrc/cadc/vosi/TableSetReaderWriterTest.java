/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.vosi;

import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.tap.schema.TestUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class TableSetReaderWriterTest {
    private static final Logger log = Logger.getLogger(TableSetReaderWriterTest.class);
    static {
        Log4jInit.setLevel("ca.nrc.cadc.vosi", Level.INFO);
    }

    String DEFAULT_SCHEMA = "default";

    public TableSetReaderWriterTest() {
    }
    
    @Test
    public final void testEmpty() {
        log.debug("testEmpty");
        try {
            TapSchema expected = new TapSchema();
            
            TableSetWriter w = new TableSetWriter();
            StringWriter out = new StringWriter();
            w.write(expected, out);
            
            Assert.fail("write empty TapSchema, expected IllegalArgumentException");
        } catch(IllegalArgumentException expected) {
            log.info("expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: "  + unexpected);
        }
    }

    @Test
    public final void testSingleTableTAP_10() {
        try {
            TapSchema expected = TestUtil.createSimpleTapSchema(10, 1, 1, 2, 2);
            
            TableSetWriter w = new TableSetWriter();
            StringWriter out = new StringWriter();
            w.write(expected, out);
            
            String xml = out.getBuffer().toString();
            log.info(" testSingleTable:\n" + xml);
                    
            StringReader in = new StringReader(xml);
            TableSetReader r = new TableSetReader(true);
            TapSchema actual = r.read(in);
            
            assertEquiv(10, expected, actual);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: "  + unexpected);
        }
    }
    
    @Test
    public final void testSingleTableTAP_11() {
        try {
            TapSchema expected = TestUtil.createSimpleTapSchema(11, 1, 1, 3, 3);
            
            TableSetWriter w = new TableSetWriter();
            StringWriter out = new StringWriter();
            w.write(expected, out);
            
            String xml = out.getBuffer().toString();
            log.info(" testSingleTable:\n" + xml);
                    
            StringReader in = new StringReader(xml);
            TableSetReader r = new TableSetReader();
            TapSchema actual = r.read(in);
            
            assertEquiv(11, expected, actual);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: "  + unexpected);
        }
    }
    
    @Test
    public final void testMultipleTablesTAP_10() {
        try {
            TapSchema expected = TestUtil.createSimpleTapSchema(10, 2, 2, 2, 2);
            
            TableSetWriter w = new TableSetWriter();
            StringWriter out = new StringWriter();
            w.write(expected, out);
            
            String xml = out.getBuffer().toString();
            log.debug(" testMultipleTables:\n" + xml);
                    
            StringReader in = new StringReader(xml);
            TableSetReader r = new TableSetReader();
            TapSchema actual = r.read(in);
            
            assertEquiv(10, expected, actual);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: "  + unexpected);
        }
    }
    
    @Test
    public final void testMultipleTablesTAP_11() {
        try {
            TapSchema expected = TestUtil.createSimpleTapSchema(11, 2, 2, 3, 3);
            
            TableSetWriter w = new TableSetWriter();
            StringWriter out = new StringWriter();
            w.write(expected, out);
            
            String xml = out.getBuffer().toString();
            log.debug(" testMultipleTables:\n" + xml);
                    
            StringReader in = new StringReader(xml);
            TableSetReader r = new TableSetReader();
            TapSchema actual = r.read(in);
            
            assertEquiv(11, expected, actual);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: "  + unexpected);
        }
    }
    
    // TODO: add comparisons of optional elements to tests: description, utypes, units, etc
    private void assertEquiv(int ver, TapSchema expected, TapSchema actual) {
        Assert.assertEquals("num schema", expected.getSchemaDescs().size(), actual.getSchemaDescs().size());
        Assert.assertEquals("num function", expected.getFunctionDescs().size(), actual.getFunctionDescs().size());

        Iterator<SchemaDesc> esi = expected.getSchemaDescs().iterator();
        Iterator<SchemaDesc> asi = actual.getSchemaDescs().iterator();
        while (esi.hasNext()) {
            SchemaDesc esd = esi.next();
            SchemaDesc asd = asi.next();
            Assert.assertEquals(esd.getSchemaName(), asd.getSchemaName());
            Assert.assertEquals("num tables in " + esd.getSchemaName(), esd.getTableDescs().size(), asd.getTableDescs().size());
            Iterator<TableDesc> eti = esd.getTableDescs().iterator();
            Iterator<TableDesc> ati = asd.getTableDescs().iterator();
            while (eti.hasNext()) {
                TableDesc etd = eti.next();
                TableDesc atd = ati.next();
                Assert.assertEquals(etd.getTableName(), atd.getTableName());
                Assert.assertEquals(etd.description, atd.description);
                Assert.assertEquals("num columns in " + etd.getTableName(), etd.getColumnDescs().size(), atd.getColumnDescs().size());
                
                Iterator<ColumnDesc> eci = etd.getColumnDescs().iterator();
                Iterator<ColumnDesc> aci = atd.getColumnDescs().iterator();
                while(eci.hasNext()) {
                    ColumnDesc ecd = eci.next();
                    ColumnDesc acd = aci.next();
                    Assert.assertEquals(ecd.getTableName(), acd.getTableName());
                    Assert.assertEquals(ecd.getColumnName(), acd.getColumnName());
                    TapDataType edt = ecd.getDatatype();
                    TapDataType adt = acd.getDatatype();
                    Assert.assertEquals(edt.getDatatype(), adt.getDatatype());
                    Assert.assertEquals(ecd.description, acd.description);
                    Assert.assertEquals(ecd.ucd, acd.ucd);
                    Assert.assertEquals(ecd.unit, acd.unit);
                    Assert.assertEquals(ecd.utype, acd.utype);
                    Assert.assertEquals(ecd.columnID, acd.columnID);
                    
                    Assert.assertEquals(ecd.getColumnName(), edt.arraysize, adt.arraysize);
                    Assert.assertEquals(ecd.getColumnName(), edt.isVarSize(), adt.isVarSize());
                    Assert.assertEquals(ecd.getColumnName(), edt.xtype, adt.xtype);
                }
            }
        }
    }

}
