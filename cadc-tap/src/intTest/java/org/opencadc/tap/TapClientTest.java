/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2020.                            (c) 2020.
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

package org.opencadc.tap;

import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class TapClientTest {
    private static final Logger log = Logger.getLogger(TapClientTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.tap", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.net", Level.INFO);
    }
    
    URI resourceID = URI.create("ivo://cadc.nrc.ca/argus");
    TapClient tapClient;
    
    public TapClientTest() throws Exception { 
        this.tapClient = new TapClient(resourceID);
    }
    
    @Test
    public void testObjectQueryOK() throws Exception {
        String query = "select schema_name,table_name,description,utype,table_type,table_index"
                + " from tap_schema.tables where table_name='tap_schema.tables'";
        TapClient<TableDesc> tc = new TapClient(resourceID);
        TableDesc td = tc.queryForObject(query, new TapSchemaTablesRowMapper());
        Assert.assertNotNull(td);
        Assert.assertEquals("tap_schema.tables", td.getTableName());
    }
    
    @Test
    public void testObjectQueryFAIL_rowcount() throws Exception {
        String query = "select schema_name,table_name,description,utype,table_type,table_index"
                + " from tap_schema.tables";
        TapClient<TableDesc> tc = new TapClient(resourceID);
        try {
            TableDesc td = tc.queryForObject(query, new TapSchemaTablesRowMapper());
            Assert.fail("expected IllegalArgumentException, got: " + td);
        } catch (IllegalArgumentException expected) {
            log.info("caught expected: " + expected);
        }
    }
    
    @Test
    public void testObjectQueryFAIL_invalid() throws Exception {
        String query = "select X,schema_name,table_name,description,utype,table_type,table_index"
                + " from tap_schema.tables where table_name='tap_schema.tables'";
        TapClient<TableDesc> tc = new TapClient(resourceID);
        try {
            TableDesc td = tc.queryForObject(query, new TapSchemaTablesRowMapper());
            Assert.fail("expected IllegalArgumentException, got: " + td);
        } catch (IllegalArgumentException expected) {
            log.info("caught expected: " + expected);
        }
    }
    
    @Test
    public void testIteratorQueryOK() throws Exception {
        String query = "select schema_name,table_name,description,utype,table_type,table_index"
                + " from tap_schema.tables where schema_name='tap_schema'"
                + " order by table_index";
        ResourceIterator<TableDesc> ti = tapClient.query(query, new TapSchemaTablesRowMapper());
        Assert.assertNotNull(ti);
        log.info("iterator impl: " + ti.getClass().getName());
        while (ti.hasNext()) {
            TableDesc td = ti.next();
            log.info(td);
        }
    }
    
    @Test
    public void testIteratorQueryOK_assumeLargeResult() throws Exception {
        String query = "select schema_name,table_name,description,utype,table_type,table_index"
                + " from tap_schema.tables where schema_name='tap_schema'"
                + " order by table_index";
        ResourceIterator<TableDesc> ti = tapClient.query(query, new TapSchemaTablesRowMapper(), true);
        Assert.assertNotNull(ti);
        log.info("iterator impl: " + ti.getClass().getName());
        while (ti.hasNext()) {
            TableDesc td = ti.next();
            log.info(td);
        }
    }
    
    @Test
    public void testIteratorQueryOK_ForceTSV() throws Exception {
        String query = "select top " + TapClient.VOTABLE_MAXREC + 10 + " collection, observationID"
                + " from caom2.Observation";
        ResourceIterator<TableDesc> ti = tapClient.query(query, new RawRowMapper());
        Assert.assertNotNull(ti);
        Assert.assertEquals(TsvIterator.class.getName(), ti.getClass().getName());
        ti.close();
    }
    
    @Test
    public void testSyncRawOK() throws Exception {
        String query = "select schema_name,table_name,description,utype,table_type,table_index"
                + " from tap_schema.tables where schema_name='tap_schema'"
                + " order by table_index";
        ResourceIterator<List<Object>> ti = tapClient.query(query, new RawRowMapper());
        Assert.assertNotNull(ti);
        log.info("iterator impl: " + ti.getClass().getName());
        while (ti.hasNext()) {
            List<Object> row = ti.next();
            StringBuilder sb = new StringBuilder();
            for (Object o : row) {
                sb.append(o).append("\t");
            }
            log.info(sb.toString());
        }
    }
    
    @Test
    public void testSyncBadQuery() throws Exception {
        String query = "select foo bar";
        
        try {
            ResourceIterator<TableDesc> ti = tapClient.query(query, new TapSchemaTablesRowMapper());
            Assert.fail("expected IllegalArgumentException, got: " + ti.getClass().getName());
        } catch (IllegalArgumentException expected) {
            log.info("caught expected: " + expected.getClass().getName());
            log.info(expected.getMessage());
        }
    }
    
    
    class TapSchemaTablesRowMapper implements TapRowMapper<TableDesc> {

        @Override
        public TableDesc mapRow(List<Object> row) {
            Iterator i = row.iterator();
            String schemaName = (String) i.next();
            String tableName = (String) i.next();
            TableDesc ret = new TableDesc(schemaName, tableName);
            ret.description = (String) i.next();
            ret.utype = (String) i.next();
            String stt = (String) i.next();
            log.debug("type enum: " + stt);
            if (stt != null) {
                ret.tableType = TableDesc.TableType.toValue(stt);
            }
            ret.tableIndex = (Integer) i.next();
            return ret;
        }
    }
}
