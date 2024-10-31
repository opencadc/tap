/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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

package ca.nrc.cadc.tap.pg;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.tap.db.TableCreator;
import ca.nrc.cadc.tap.db.TableLoader;
import ca.nrc.cadc.tap.pg.TableCreatorTest.SimpleRowMapper;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.util.List;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.tap.io.AsciiTableData;
import org.springframework.jdbc.core.JdbcTemplate;

public class TableLoaderTest extends TestUtil {

    private static final Logger log = Logger.getLogger(TableLoaderTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.pg", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.tap.db", Level.INFO);
    }
    
    public TableLoaderTest() { 
        super();
    }
    
    @Test
    public void testLoadDALI() {
        try {
            String testTable = testSchemaName + ".testLoadDALI";
            TableDesc orig = new TableDesc(testSchemaName, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "e7", TapDataType.INTERVAL));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "e8", TapDataType.POINT));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "e9", TapDataType.CIRCLE));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "e10", TapDataType.POLYGON));
            
            TableCreator tc = new TableCreator(dataSource);
            try {
                tc.dropTable(testTable);
            } catch (Exception ignore) {
            }
            log.info("createTable...");
            tc.createTable(orig);
            log.info("createTable... [OK]");
            
            StringBuilder csvData = new StringBuilder();
            csvData.append("e7, e8, e9, e10\n");
            csvData.append("1.0 2.0,12.0 34.0,12.0 34.0 0.5,10.0 11.0 9.0 13.0 11.0 12.0\n");
            
            AsciiTableData tw = new AsciiTableData(new ByteArrayInputStream(csvData.toString().getBytes()), "text/csv");
            TableLoader tableLoader = new TableLoader(dataSource, 3);
            log.info("load...");
            tableLoader.load(orig, tw);
            log.info("load... [OK]");
            
            String sql = "SELECT * from " + testTable;
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            List<List<Object>> rows = jdbc.query(sql, new SimpleRowMapper());
            Assert.assertNotNull("rows", rows);
            Assert.assertTrue("count", rows.size() == 1);
            List<Object> row = rows.get(0);
            for (Object o : row) {
                log.info("value: " + o);
                Assert.assertNotNull(o);
            }
            
            // cleanup
            tc.dropTable(testTable);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testLoadArrays() {
        try {
            String testTable = testSchemaName + ".testLoadArrays";
            TableDesc orig = new TableDesc(testSchemaName, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "a11", new TapDataType("short", "4", null)));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "a12", new TapDataType("int", "*", null)));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "a13", new TapDataType("long", "*", null)));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "a14", new TapDataType("float", "4", null)));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "a15", new TapDataType("double", "*", null)));
            
            TableCreator tc = new TableCreator(dataSource);
            try {
                tc.dropTable(testTable);
            } catch (Exception ignore) {
            }
            log.info("createTable...");
            tc.createTable(orig);
            log.info("createTable... [OK]");
            
            StringBuilder csvData = new StringBuilder();
            csvData.append("a11,a12,a13,a14,a15\n");
            csvData.append("1 2 3 4,11 12 13 14,21 22 23 24,5.0 6.0 7.0 8.0,15.0 16.0 17.0 18.0\n");
            
            AsciiTableData tw = new AsciiTableData(new ByteArrayInputStream(csvData.toString().getBytes()), "text/csv");
            TableLoader tableLoader = new TableLoader(dataSource, 3);
            log.info("load...");
            tableLoader.load(orig, tw);
            log.info("load... [OK]");
            
            String sql = "SELECT * from " + testTable;
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            List<List<Object>> rows = jdbc.query(sql, new SimpleRowMapper());
            Assert.assertNotNull("rows", rows);
            Assert.assertTrue("count", rows.size() == 1);
            List<Object> row = rows.get(0);
            for (Object o : row) {
                log.info("value: " + o);
                Assert.assertNotNull(o);
            }
            
            // cleanup
            tc.dropTable(testTable);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
}
