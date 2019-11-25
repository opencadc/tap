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

package ca.nrc.cadc.tap.parser.converter;

import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Convert column name in the query to actual column name in tables
 *
 * @author pdowler
 */
public class ColumnNameConverterTest {

    private static final Logger log = Logger.getLogger(ColumnNameConverterTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.parser", Level.INFO);
    }

    public ColumnNameConverterTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public final void testCaseInsensitiveCompatUnqualified() {
        String[] cols = {"oldColumn", "OldColumn", "oldcolumn"};
        try {
            for (String c : cols) {
                String query = "select " + c + " from schemaX.tableX";
                String sql = convert("testCaseInsensitiveCompatUnqualified", query, true, false);
                Assert.assertTrue("testCaseInsensitiveCompatUnqualified: ", sql.contains("newColumn"));
            }
        } catch (Exception t) {
            log.error("testCaseInsensitiveCompatUnqualified: " + t);
            Assert.assertFalse(true);
        }
    }
    
    @Test
    public final void testCaseInsensitive() {
        String[] cols = {"oldColumn", "OldColumn", "oldcolumn"};
        try {
            for (String c : cols) {
                String query = "select " + c + " from schemaX.tableX";
                String sql = convert("testSelectCaseInsensitive", query, true, true);
                Assert.assertTrue("testSelectCaseInsensitive: something", sql.contains("newColumn"));
            }
        } catch (Exception t) {
            log.error("testSelectCaseInsensitive: " + t);
            Assert.assertFalse(true);
        }
    }
    
    @Test
    public final void testCaseInsensitiveAlias() {
        String[] cols = {"oldColumn", "OldColumn", "oldcolumn"};
        try {
            for (String c : cols) {
                String query = "select s." + c + " from schemaX.tableX AS s";
                String sql = convert("testSelectCaseInsensitive", query, true, true);
                Assert.assertTrue("testSelectCaseInsensitive: something", sql.contains("s.newColumn"));
            }
        } catch (Exception t) {
            log.error("testSelectCaseInsensitive: " + t);
            Assert.assertFalse(true);
        }
    }

    @Test
    public final void testCaseSensitive() {
        try {
            String query = "select oldColumn from schemaX.tableX";
            String sql = convert("testCaseSensitive", query, false, true);
            Assert.assertTrue("testCaseSensitive: !oldColumn", !sql.contains("oldColumn"));
            Assert.assertTrue("testCaseSensitive: newColumn", sql.contains("newColumn"));

            query = "select OLDCOLUMN from schemaX.tableX";
            sql = convert("testCaseSensitive", query, false, true);
            Assert.assertTrue("testCaseSensitive: OLDCOLUMN", sql.contains("OLDCOLUMN"));
            Assert.assertTrue("testCaseSensitive: !newColumn", !sql.contains("newColumn"));
        } catch (Exception t) {
            log.error("testCaseSensitive: " + t);
            Assert.assertFalse(true);
        }
    }
    
    @Test
    public final void testCaseSensitiveAlias() {
        try {
            String query = "select s.oldColumn from schemaX.tableX AS s";
            String sql = convert("testCaseSensitive", query, false, true);
            Assert.assertTrue("testCaseSensitive: !oldColumn", !sql.contains("oldColumn"));
            Assert.assertTrue("testCaseSensitive: newColumn", sql.contains("s.newColumn"));

            query = "select s.OLDCOLUMN from schemaX.tableX AS s";
            sql = convert("testCaseSensitive", query, false, true);
            Assert.assertTrue("testCaseSensitive: OLDCOLUMN", sql.contains("s.OLDCOLUMN"));
            Assert.assertTrue("testCaseSensitive: !newColumn", !sql.contains("newColumn"));
        } catch (Exception t) {
            log.error("testCaseSensitive: " + t);
            Assert.assertFalse(true);
        }
    }
    

    @Test
    public final void testAlias() {
        try {
            String query = "select a.oldColumn from schemaX.tableX AS a";
            String sql = convert("testAlias", query, false, true);
            Assert.assertTrue("testWhere: something", !sql.contains("oldColumn"));
            Assert.assertTrue("testWhere: something", sql.contains("a.newColumn"));
        } catch (Exception t) {
            log.error("testAlias: " + t);
            Assert.assertFalse(true);
        }
    }

    @Test
    public final void testWhere() {
        try {
            String query = "select * from schemaX.tableX WHERE oldColumn is not null";
            String sql = convert("testWhere", query, false, true);
            Assert.assertTrue("testWhere: !oldColumn", !sql.contains("oldColumn"));
            Assert.assertTrue("testWhere: newColumn", sql.contains("newColumn"));
        } catch (Exception t) {
            log.error("testWhere: " + t);
            Assert.assertFalse(true);
        }
    }

    @Test
    public final void testJoin() {
        try {
            String query = "select * from schemaX.tableX as st JOIN schemaX.otherX as ot on st.oldColumn = ot.oldColumn2";
            String sql = convert("testJoin", query, false, true);
            Assert.assertTrue("testJoin: something", !sql.contains("oldColumn"));
            Assert.assertTrue("testJoin: something", sql.contains("st.newColumn"));
            Assert.assertTrue("testJoin: something", sql.contains("ot.newColumn2"));
        } catch (Exception t) {
            log.error("testJoin: " + t);
            Assert.assertFalse(true);
        }
    }

    // TODO: group by, having, subquery
    private String convert(String test, String query, boolean ignoreCase, boolean qualified) {
        try {
            job.getParameterList().add(new Parameter("QUERY", query));
            log.info(test + ", before: " + query);
            TestQuery tq = new TestQuery();
            tq.ignoreCase = ignoreCase;
            tq.qualified = qualified;
            tq.setJob(job);
            String sql = tq.getSQL();
            log.info(test + ", after: " + sql);
            return sql;
        } finally {
            job.getParameterList().clear();
        }
    }

    Job job = new Job() {
        @Override
        public String getID() {
            return "abcdefg";
        }
    };

    static class TestQuery extends AdqlQuery {

        boolean ignoreCase;
        boolean qualified;

        protected void init() {
            //super.init();
            TapSchema ts = new TapSchema();
            SchemaDesc sd = new SchemaDesc("schemaX");
            TableDesc td1 = new TableDesc(sd.getSchemaName(), sd.getSchemaName() + ".tableX");
            ColumnDesc cd1 = new ColumnDesc(td1.getTableName(), "oldColumn", TapDataType.STRING);
            td1.getColumnDescs().add(cd1);
            sd.getTableDescs().add(td1);
            
            TableDesc td2 = new TableDesc(sd.getSchemaName(), sd.getSchemaName() + ".otherX");
            ColumnDesc cd2 = new ColumnDesc(td2.getTableName(), "oldColumn2", TapDataType.STRING);
            td2.getColumnDescs().add(cd2);
            sd.getTableDescs().add(td2);
            
            ts.getSchemaDescs().add(sd);
            
            ColumnNameConverter cnc = new ColumnNameConverter(ignoreCase, ts);
            if (qualified) {
                cnc.put(new ColumnNameConverter.QualifiedColumn(td1.getTableName(), "oldColumn"), 
                    new ColumnNameConverter.QualifiedColumn(td1.getTableName(), "newColumn"));
                cnc.put(new ColumnNameConverter.QualifiedColumn(td2.getTableName(), "oldColumn2"), 
                    new ColumnNameConverter.QualifiedColumn(td2.getTableName(), "newColumn2"));
            } else {
                cnc.put("oldColumn", "newColumn");
            }
            SelectNavigator sn = new SelectNavigator(new ExpressionNavigator(), cnc, new FromItemNavigator());
            super.navigatorList.add(sn);
        }
    }
    
}
