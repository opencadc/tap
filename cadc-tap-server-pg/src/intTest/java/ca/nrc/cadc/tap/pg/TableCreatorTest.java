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
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.util.Log4jInit;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
public class TableCreatorTest extends TestUtil {
    private static final Logger log = Logger.getLogger(TableCreatorTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.pg", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.tap.db", Level.INFO);
        //Log4jInit.setLevel("ca.nrc.cadc.profiler", Level.INFO);
    }
    
    public TableCreatorTest() { 
        super();
    }
    
    @Test
    public void testCreateTable() {
        try {
            String testTable = testSchemaName + ".testCreateTable";
            // cleanup
            TableCreator tc = new TableCreator(dataSource);
            try {
                tc.dropTable(testTable);
            } catch (Exception ignore) {
                log.debug("cleanup-before-test failed");
            }
            
            TableDesc orig = new TableDesc(testSchemaName, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "e7", TapDataType.INTERVAL));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "e8", TapDataType.POINT));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "e9", TapDataType.CIRCLE));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "e10", TapDataType.POLYGON));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "a11", new TapDataType("int", "8", null))); // int[8]
            orig.getColumnDescs().add(new ColumnDesc(testTable, "a12", new TapDataType("double", "8", null))); // double[8]

            tc.createTable(orig);
            log.info("createTable returned");
            
            String sql = "SELECT * from " + testTable;
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            List<List<Object>> rows = jdbc.query(sql, new SimpleRowMapper());
            Assert.assertNotNull("rows", rows);
            Assert.assertTrue("empty", rows.isEmpty());
            log.info("queries empty table");
            
            // cleanup
            tc.dropTable(testTable);
            log.info("dropped table");
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCreateIndex() {
        try {
            String testTable = testSchemaName + ".testCreateIndex";
            // cleanup
            TableCreator tc = new TableCreator(dataSource);
            try {
                tc.dropTable(testTable);
            } catch (Exception ignore) {
                log.debug("cleanup-before-test failed");
            }
            
            TableDesc orig = new TableDesc(testSchemaName, testTable);
            orig.tableType = TableDesc.TableType.TABLE;
            orig.getColumnDescs().add(new ColumnDesc(testTable, "e7", TapDataType.INTERVAL));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "e8", TapDataType.POINT));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "e9", TapDataType.CIRCLE));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "e10", TapDataType.POLYGON));
            orig.getColumnDescs().add(new ColumnDesc(testTable, "a11", new TapDataType("int", "8", null))); // int[8]
            orig.getColumnDescs().add(new ColumnDesc(testTable, "a12", new TapDataType("double", "8", null))); // double[8]

            tc.createTable(orig);
            log.info("createTable returned");
            
            for (ColumnDesc cd : orig.getColumnDescs()) {
                if (cd.getColumnName().charAt(0) == 'e') {
                    // unique not supported
                    try {
                        tc.createIndex(cd, true);
                    } catch (IllegalArgumentException expected) {
                        log.info("caught expected: " + expected);
                    }
                    // regular index supported
                    tc.createIndex(cd, false);
                } else if (cd.getColumnName().charAt(0) == 'a') {
                    // index not supported
                    try {
                        tc.createIndex(cd, false);
                    } catch (IllegalArgumentException expected) {
                        log.info("caught expected: " + expected);
                    }
                }
            }
            
            // cleanup
            tc.dropTable(testTable);
            log.info("dropped table");
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    public static class SimpleRowMapper implements RowMapper {

        @Override
        public Object mapRow(ResultSet rs, int i) throws SQLException {
            int num = rs.getMetaData().getColumnCount();
            List<Object> ret = new ArrayList<Object>(num);
            for (int c = 1; c <= num; c++) {
                ret.add(rs.getObject(c));
            }
            return ret;
        }
        
    }
}
