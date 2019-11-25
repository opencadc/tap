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
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import ca.nrc.cadc.util.CaseInsensitiveStringComparator;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.util.Map;
import java.util.TreeMap;
import net.sf.jsqlparser.schema.Table;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class TableNameConverterTest {

    private static final Logger log = Logger.getLogger(TableNameConverterTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.parser", Level.INFO);
    }

    public TableNameConverterTest() {
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
    public final void testCaseInsensitive() {
        String[] cols = {"oldTable", "OldTable", "oldtable"};
        try {
            for (String c : cols) {
                String query = "select * from " + c;
                String sql = convert("testSelectCaseInsensitive", query, ot, nt, true);
                Assert.assertTrue("testSelectCaseInsensitive: something", sql.contains(nt));
            }
        } catch (Throwable t) {
            log.error("testSelectCaseInsensitive: ", t);
            Assert.assertFalse(true);
        }
    }

    @Test
    public final void testCaseSensitive() {

        try {
            String query = "select * from oldTable";
            String sql = convert("testCaseSensitive", query, ot, nt, false);
            Assert.assertTrue("testCaseSensitive: !oldTable", !sql.contains(ot));
            Assert.assertTrue("testCaseSensitive: newTable", sql.contains(nt));

            query = "select * from OLDTABLE";
            sql = convert("testCaseSensitive", query, ot, nt, false);
            Assert.assertTrue("testCaseSensitive: OLDTABLE", sql.contains("OLDTABLE"));
            Assert.assertTrue("testCaseSensitive: !newTable", !sql.contains("newTable"));
        } catch (Exception t) {
            log.error("testCaseSensitive: ", t);
            Assert.assertFalse(true);
        }
    }

    @Test
    public final void testWithSchema() {
        try {
            String ost = "someSchema." + ot;
            String nst = "someSchema." + nt;
            String query = "select * from " + ost;
            String sql = convert("testWithSchema", query, ost, nst, false);
            Assert.assertTrue("testWithSchema: something", !sql.contains(ost));
            Assert.assertTrue("testWithSchema: something", sql.contains(nst));
        } catch (Throwable t) {
            log.error("testWithSchema: ", t);
            Assert.assertFalse(true);
        }
    }

    @Test
    public final void testWithSchemaChange() {
        try {
            String ost = "oldSchema." + ot;
            String nst = "newSchema." + ot;
            String query = "select * from " + ost;
            String sql = convert("testWithSchemaChange", query, ost, nst, false);
            Assert.assertTrue("testWithSchemaChange: something", !sql.contains(ost));
            Assert.assertTrue("testWithSchemaChange: something", sql.contains(nst));
        } catch (Throwable t) {
            log.error("testWithSchemaChange: ", t);
            Assert.assertFalse(true);
        }
    }

    @Test
    public final void testChangeBoth() {
        try {
            String ost = "oldSchema." + ot;
            String nst = "newSchema." + nt;
            String query = "select * from " + ost;
            String sql = convert("testChangeBoth", query, ost, nst, false);
            Assert.assertTrue("testChangeBoth: something", !sql.contains(ost));
            Assert.assertTrue("testChangeBoth: something", sql.contains(nst));
        } catch (Throwable t) {
            log.error("testChangeBoth: ", t);
            Assert.assertFalse(true);
        }
    }

    @Test
    public final void testFullyQualifiedColumnName() {
        try {
            String otn = "someSchema." + ot;
            String ocn = otn + ".someColumn";
            String ntn = "someSchema." + nt;
            String ncn = ntn + ".someColumn";

            String query = "select " + ocn + " from " + otn + " order by " + ocn;
            String sql = convert("testFullyQualifiedColumnName", query, otn, ntn, false);
            Assert.assertTrue("no old table name", !sql.contains(otn));
            Assert.assertTrue("new table name", sql.contains(ntn));
            Assert.assertTrue("new fully qualified column name", sql.contains(ncn));
        } catch (Throwable t) {
            log.error("testFullyQualifiedColumnName: ", t);
            Assert.assertFalse(true);
        }
    }

    @Test
    public final void testSubQuery() {
        try {
            String query = "select * from foo where bar in (select bar from " + ot + ")";
            String sql = convert("testSubQuery", query, ot, nt, false);
            Assert.assertTrue("testSubQuery: something", !sql.contains(ot));
            Assert.assertTrue("testSubQuery: something", sql.contains(nt));
        } catch (Throwable t) {
            log.error("testSubQuery: ", t);
            Assert.assertFalse(true);
        }
    }

    @Test
    public final void testDatabaseSchemaTable() {
        try {
            String ost = "someDB.oldSchema." + ot;
            String nst = "someDB.newSchema." + ot;
            String query = "select * from " + ost;
            String sql = convert("testDatabaseSchemaTable", query, ost, nst, false);
            Assert.fail("testDatabaseSchemaTable: expected an exception here");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue("testDatabaseSchemaTable: caught expected exception: " + expected, true);
        } catch (Throwable t) {
            log.error("testDatabaseSchemaTable: ", t);
            Assert.fail("testDatabaseSchemaTable: " + t);
        }
    }

    private String convert(String test, String query, String ot, String nt, boolean ignoreCase) {
        try {
            job.getParameterList().add(new Parameter("QUERY", query));
            log.debug(test + ", before: " + query);
            TestQuery tq = new TestQuery(ot, nt, ignoreCase);
            tq.setJob(job);
            String sql = tq.getSQL();
            log.debug(test + ", after: " + sql);
            return sql;
        } finally {
            job.getParameterList().clear();
        }
    }

    String ot = "oldTable";
    String nt = "newTable";

    Job job = new Job() {
        @Override
        public String getID() {
            return "abcdefg";
        }
    };

    static class TestQuery extends AdqlQuery {

        TableNameConverter tnc;
        TableNameReferenceConverter tnrc;

        TestQuery(String oldName, String newName, boolean ignoreCase) {
            Table t = new Table();
            String[] parts = newName.split("[.]");
            if (parts.length == 1) {
                t.setName(parts[0]);
            } else if (parts.length == 2) {
                t.setSchemaName(parts[0]);
                t.setName(parts[1]);
            } else {
                throw new IllegalArgumentException("expected new table name to have 1-2 parts, found " + parts.length);
            }

            Map m = null;
            if (ignoreCase) {
                m = new TreeMap<String, Table>(new CaseInsensitiveStringComparator());
            } else {
                m = new TreeMap<String, Table>();
            }
            m.put(oldName, t);

            this.tnc = new TableNameConverter(m);
            this.tnrc = new TableNameReferenceConverter(m);
            //tnc.put(oldName, newName);
            //tnc.put(oldName, newName);
        }

        protected void init() {
            SelectNavigator sn = new SelectNavigator(
                    new ExpressionNavigator(), tnrc, tnc);
            super.navigatorList.add(sn);
        }
    }

}
