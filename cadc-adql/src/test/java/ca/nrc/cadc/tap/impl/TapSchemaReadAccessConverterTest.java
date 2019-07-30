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

package ca.nrc.cadc.tap.impl;

import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencadc.gms.GroupClient;
import org.opencadc.gms.GroupURI;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.auth.NumericPrincipal;
import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.TapQuery;
import ca.nrc.cadc.tap.impl.TapSchemaReadAccessConverter;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;

/**
 * Test class for the CaomReadAccessConverter.
 *
 * @author pdowler, majorb
 */
public class TapSchemaReadAccessConverterTest {

    private static final Logger log = Logger.getLogger(TapSchemaReadAccessConverterTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.schema", Level.INFO);
    }

    static String[] ASSET_TABLES = new String[] {
        "tap_schema.schemas".toLowerCase(),
        "tap_schema.tables".toLowerCase(),
        "tap_schema.columns".toLowerCase(),};
    
    static String[] ASSET_TABLE_CONDITION_TABLES = new String[] {
        "tap_schema.schemas".toLowerCase(),
        "tap_schema.tables".toLowerCase(),
        "tap_schema.tables".toLowerCase(),};

    static String[] KEY_COLUMNS = new String[] {
        "schema_name".toLowerCase(),
        "table_name".toLowerCase(),
        "table_name".toLowerCase(),};

    static String[] OWNER_COLUMNS = new String[] {
        "owner_id".toLowerCase(),
        "owner_id".toLowerCase(),
        "owner_id".toLowerCase(),};
    
    static String[] PUBLIC_COLUMNS = new String[] {
        "read_anon".toLowerCase(),
        "read_anon".toLowerCase(),
        "read_anon".toLowerCase(),};
    
    static String[] READGROUP_COLUMNS = new String[] {
        "read_only_group".toLowerCase(),
        "read_only_group".toLowerCase(),
        "read_only_group".toLowerCase(),};
    
    static String[] READWRITEGROUP_COLUMNS = new String[] {
        "read_write_group".toLowerCase(),
        "read_write_group".toLowerCase(),
        "read_write_group".toLowerCase(),};
    
    static long userIDWithGroups = 1L;
    static long userIDWithNoGroups = 2L;
    
    static GroupURI group1 = new GroupURI("ivo://cadc.nrc.ca/gms?666");
    static GroupURI group2 = new GroupURI("ivo://cadc.nrc.ca/gms?777");
    
    static String groupInExpr = "in ('" + group1.toString() + "', '" + group2.toString() + "')";

    static NumericPrincipal userWithGroups;
    static NumericPrincipal userWithNoGroups;

    static Subject subjectWithGroups;
    static Subject subjectWithNoGroups;

    public TapSchemaReadAccessConverterTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        subjectWithNoGroups = new Subject();
        userWithNoGroups = new NumericPrincipal(new UUID(0L, userIDWithNoGroups));
        subjectWithNoGroups.getPrincipals().add(TapSchemaReadAccessConverterTest.userWithNoGroups);
        log.debug("created subjectWithNoGroups: " + TapSchemaReadAccessConverterTest.subjectWithNoGroups);

        subjectWithGroups = new Subject();
        userWithGroups = new NumericPrincipal(new UUID(0L, (long) userIDWithGroups));
        subjectWithGroups.getPrincipals().add(TapSchemaReadAccessConverterTest.userWithGroups);
        log.debug("created subjectWithGroups: " + TapSchemaReadAccessConverterTest.subjectWithGroups);
    }

    private static Job job = new Job() {
        @Override
        public String getID() {
            return "internal-test-jobID";
        }
    };

    private class QueryConvertAction implements PrivilegedExceptionAction<String> {

        public String test;
        public String adql;
        boolean toLower;

        public QueryConvertAction(String test, String adql) {
            this(test, adql, true);
        }

        public QueryConvertAction(String test, String adql, boolean toLower) {
            this.test = test;
            this.adql = adql;
            this.toLower = toLower;
        }

        @Override
        public String run()
                throws Exception {
            return doIt(test, adql, toLower);
        }

    }
    
    @Test
    public final void testAnonymousNonAssetTable() {
        String method = "testAnonymousNonAssetTable";
        String tname = "usertable";
        log.info(method + ": " + tname);
        String query = "select * from " + tname;
        try {
            String sql = new QueryConvertAction(method, query).run();
            Assert.assertEquals(query, sql);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testAnonymous() {
        String method = "testAnonymous";
        for (int a = 0; a < ASSET_TABLES.length; a++) {
            String tname = ASSET_TABLES[a];
            String conditionTable = ASSET_TABLE_CONDITION_TABLES[a];
            String keyCol = conditionTable + "." + KEY_COLUMNS[a];
            String ownerCol = conditionTable + "." + OWNER_COLUMNS[a];
            String publicCol = conditionTable + "." + PUBLIC_COLUMNS[a];
            log.info(method + ": " + tname);
            try {
                String query = "select * from " + tname;
                String sql = new QueryConvertAction(method, query).run();

                int i = sql.indexOf("where");
                log.debug("i=" + i);
                Assert.assertTrue("found where", (i > 0));

                String where = sql.substring(i).toLowerCase();
                log.info(method + ": " + where);

                Assert.assertTrue(method + " " + tname + " " + keyCol, where.contains(keyCol + " is null"));
                Assert.assertTrue(method + " " + tname + " " + ownerCol, where.contains(ownerCol + " is null"));
                Assert.assertTrue(method + " " + tname + " " + publicCol, where.contains(publicCol + " = 1"));
                Assert.assertTrue(method + " " + tname + " " + ownerCol, !where.contains(ownerCol + " = '"));
            } catch (Exception unexpected) {
                log.error("unexpected exception", unexpected);
                Assert.fail("unexpected exception: " + unexpected);
            }
        }
    }
    
    @Test
    public final void testWithNoGroups() {
        String method = "testWithNoGroups";
        for (int a = 0; a < ASSET_TABLES.length; a++) {
            String tname = ASSET_TABLES[a];          
            String conditionTable = ASSET_TABLE_CONDITION_TABLES[a];
            String keyCol = conditionTable + "." + KEY_COLUMNS[a];
            String ownerCol = conditionTable + "." + OWNER_COLUMNS[a];
            String publicCol = conditionTable + "." + PUBLIC_COLUMNS[a];
            String readGroupCol = conditionTable + "." + READGROUP_COLUMNS[a];
            String readWriteGroupCol = conditionTable + "." + READWRITEGROUP_COLUMNS[a];
            log.info(method + ": " + tname);
            try {
                String query = "select * from " + tname;
                String sql = Subject.doAs(subjectWithNoGroups, new QueryConvertAction(method, query));

                int i = sql.indexOf("where");
                Assert.assertTrue("found where", (i > 0));

                String where = sql.substring(i).toLowerCase();
                log.info(method + ": " + where);

                Assert.assertTrue(method + " " + tname + " " + keyCol, where.contains(keyCol + " is null"));
                Assert.assertTrue(method + " " + tname + " " + ownerCol, where.contains(ownerCol + " is null"));
                Assert.assertTrue(method + " " + tname + " " + publicCol, where.contains(publicCol + " = 1"));
                Assert.assertTrue(method + " " + tname + " " + ownerCol, where.contains(ownerCol + " = '" + userIDWithNoGroups + "'"));
                Assert.assertTrue(method + " " + tname + " " + readGroupCol, !where.contains(readGroupCol + " in ("));
                Assert.assertTrue(method + " " + tname + " " + readWriteGroupCol, !where.contains(readWriteGroupCol + " in ("));
            } catch (Exception unexpected) {
                log.error("unexpected exception", unexpected);
                Assert.fail("unexpected exception: " + unexpected);
            }
        }
    }
    
    @Test
    public final void testWithGroups() {
        String method = "testWithGroups";
        for (int a = 0; a < ASSET_TABLES.length; a++) {
            String tname = ASSET_TABLES[a];
            String conditionTable = ASSET_TABLE_CONDITION_TABLES[a];
            String keyCol = conditionTable + "." + KEY_COLUMNS[a];
            String ownerCol = conditionTable + "." + OWNER_COLUMNS[a];
            String publicCol = conditionTable + "." + PUBLIC_COLUMNS[a];
            String readGroupCol = conditionTable + "." + READGROUP_COLUMNS[a];
            String readWriteGroupCol = conditionTable + "." + READWRITEGROUP_COLUMNS[a];
            log.info(method + ": " + tname);
            try {
                String query = "select * from " + tname;
                String sql = Subject.doAs(subjectWithGroups, new QueryConvertAction(method, query));

                int i = sql.indexOf("where");
                Assert.assertTrue("found where", (i > 0));

                String where = sql.substring(i).toLowerCase();
                log.info(method + ": " + where);

                Assert.assertTrue(method + " " + tname + " " + keyCol, where.contains(keyCol + " is null"));
                Assert.assertTrue(method + " " + tname + " " + ownerCol, where.contains(ownerCol + " is null"));
                Assert.assertTrue(method + " " + tname + " " + publicCol, where.contains(publicCol + " = 1"));
                Assert.assertTrue(method + " " + tname + " " + ownerCol, where.contains(ownerCol + " = '" + userIDWithGroups + "'"));
                Assert.assertTrue(method + " " + tname + " " + readGroupCol, where.contains(readGroupCol + " " + groupInExpr));
                Assert.assertTrue(method + " " + tname + " " + readWriteGroupCol, where.contains(readWriteGroupCol + " " + groupInExpr));
            } catch (Exception unexpected) {
                log.error("unexpected exception", unexpected);
                Assert.fail("unexpected exception: " + unexpected);
            }
        }
    }
    
    @Test
    public final void testWithWhere() {
        String method = "testWithWhere";
        for (int a = 0; a < ASSET_TABLES.length; a++) {
            String tname = ASSET_TABLES[a];
            String conditionTable = ASSET_TABLE_CONDITION_TABLES[a];
            String keyCol = conditionTable + "." + KEY_COLUMNS[a];
            String ownerCol = conditionTable + "." + OWNER_COLUMNS[a];
            String publicCol = conditionTable + "." + PUBLIC_COLUMNS[a];
            String readGroupCol = conditionTable + "." + READGROUP_COLUMNS[a];
            String readWriteGroupCol = conditionTable + "." + READWRITEGROUP_COLUMNS[a];
            log.info(method + ": " + tname);
            try {
                String query = "select * from " + tname + " where something='foo'";
                String sql = Subject.doAs(subjectWithGroups, new QueryConvertAction(method, query));

                int i = sql.indexOf("where");
                Assert.assertTrue("found where", (i > 0));

                String where = sql.substring(i).toLowerCase();
                log.info(method + ": " + where);

                Assert.assertTrue(method + " something", where.contains("something = 'foo'")); 
                
                Assert.assertTrue(method + " " + tname + " " + keyCol, where.contains(keyCol + " is null"));
                Assert.assertTrue(method + " " + tname + " " + ownerCol, where.contains(ownerCol + " is null"));
                Assert.assertTrue(method + " " + tname + " " + publicCol, where.contains(publicCol + " = 1"));
                Assert.assertTrue(method + " " + tname + " " + ownerCol, where.contains(ownerCol + " = '" + userIDWithGroups + "'"));
                Assert.assertTrue(method + " " + tname + " " + readGroupCol, where.contains(readGroupCol + " " + groupInExpr));
                Assert.assertTrue(method + " " + tname + " " + readWriteGroupCol, where.contains(readWriteGroupCol + " " + groupInExpr));
            } catch (Exception unexpected) {
                log.error("unexpected exception", unexpected);
                Assert.fail("unexpected exception: " + unexpected);
            }
        }
    }
    
    @Test
    public final void testMultiTable() {
        String method = "testMultiTable";
        try {
            String query = "select t.*, c.*"
                    + " from tap_schema.tables as t join tap_schema.columns as c on t.table_name=c.table_name"
                    + " where something='FOO'";
            String sql = Subject.doAs(subjectWithGroups, new QueryConvertAction(method, query));

            int i = sql.indexOf("where");
            Assert.assertTrue("found where", (i > 0));

            String where = sql.substring(i).toLowerCase();
            log.info(method + ": " + where);

            Assert.assertTrue(method + " something", where.contains("something = 'foo'")); // parse/deparse normalises whitespace

            Assert.assertTrue(method + " tables keyCol is null", where.contains("tables.table_name is null"));
            Assert.assertTrue(method + " tables read_anon", where.contains("tables.read_anon = 1"));
            Assert.assertTrue(method + " tables read_only_group", where.contains("tables.read_only_group " + groupInExpr));

            Assert.assertTrue(method + " columns keyCol is null", !where.contains("columns.columns_name is null"));
            Assert.assertTrue(method + " columns read_anon", !where.contains("columns.read_anon = 1"));
            Assert.assertTrue(method + " columns read_only_group", !where.contains("columns.read_only_group " + groupInExpr));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public final void testSubQueryWhere() {
        String method = "testSubQueryWhere";
        try {
            String query = "select c.*"
                    + " from tap_schema.columns as c"
                    + " where c.table_name in (select table_name from tap_schema.tables as t)";
            String sql = Subject.doAs(subjectWithGroups, new QueryConvertAction(method, query));

            int i = sql.indexOf("where");
            Assert.assertTrue("found where", (i > 0));

            String where = sql.substring(i).toLowerCase();
            log.info(method + ": " + where);

            Assert.assertTrue(method + " tables keyCol is null", where.contains("tables.table_name is null"));
            Assert.assertTrue(method + " tables public", where.contains("tables.read_anon = 1"));
            Assert.assertTrue(method + " tables groups", where.contains("tables.read_only_group " + groupInExpr));

            Assert.assertTrue(method + " columns keyCol is null", !where.contains("columns.column_name is null"));
            Assert.assertTrue(method + " columns public", !where.contains("columns.read_anon = 1"));
            Assert.assertTrue(method + " columns groups", !where.contains("columns.read_only_group " + groupInExpr));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private String doIt(String testName, String query, boolean toLower)
            throws Exception {
        try {
            List<Parameter> params = new ArrayList<Parameter>();
            params.add(new Parameter("QUERY", query));
            log.debug(testName + " before: " + query);
            TapQuery tq = new TestQuery();
            job.getParameterList().addAll(params);
            tq.setJob(job);
            String sql = tq.getSQL();
            log.debug(testName + " after : " + sql);
            if (toLower) {
                return sql.toLowerCase();
            }
            return sql;
        } finally {
            job.getParameterList().clear();
        }
    }

    // expect: $keyCol IS NULL OR $groupsCol @@ 'g1 g2 ...'
    private void assertGroupRestriction(String method, String where, String keyCol, String groupsCol, String alias) {
        log.debug("assertGroupRestriction: " + where + " vs. " + keyCol + " and " + groupsCol);
        if (keyCol != null) {
            Assert.assertTrue(method + "[" + where + "] has key column " + keyCol, where.contains(keyCol + " is null or "));
        }

        String gc = groupsCol;
        if (alias != null) {
            gc = alias + "." + groupsCol;
        }
        Assert.assertTrue(method + "[" + where + "] has meta table " + groupsCol, where.contains(gc + " @@ "));
        Assert.assertTrue(method + "[" + where + "] has meta table " + groupsCol, where.contains("::tsquery"));

        Assert.assertTrue(method + "[" + where + "] has meta read access group 666", where.contains("666"));
        Assert.assertTrue(method + "[" + where + "] has meta read access group 777", where.contains("777"));
    }

    class TestQuery extends AdqlQuery {

        boolean auth;

        TestQuery() {
            this(true);
        }

        TestQuery(boolean auth) {
            super();
            this.auth = auth;
        }

        @Override
        protected void init() {
            // no super.init() on purpose so we only have one navigator in list
            TapSchemaReadAccessConverter rac = new TestTapSchemaReadAccessConverter(new TestIdentityManager());
            rac.setGroupClient(new TestGMSClient());
            super.navigatorList.add(rac);
        }
    }
    
    static class TestIdentityManager implements IdentityManager {

        @Override
        public Subject toSubject(Object owner) {
            return null;
        }

        @Override
        public Object toOwner(Subject subject) {
            return null;
        }

        @Override
        public int getOwnerType() {
            return 0;
        }

        @Override
        public String toOwnerString(Subject subject) {
            if (subject == null) {
                return null;
            }
            Set<NumericPrincipal> p = subject.getPrincipals(NumericPrincipal.class);
            if (p.size() > 0) {
                UUID uuid = UUID.fromString(p.iterator().next().getName());
                return new Long(uuid.getLeastSignificantBits()).toString();
            }
            return null;
        }
        
    }

    static class TestGMSClient implements GroupClient {

        @Override
        public List<GroupURI> getMemberships() {
            Subject cur = AuthenticationUtil.getCurrentSubject();

            List<GroupURI> memberships = new ArrayList<GroupURI>();
            log.info("Current subject: " + cur);
            log.info("Subject with groups: " + subjectWithGroups);
            if (cur == subjectWithGroups) {
                memberships.add(group1);
                memberships.add(group2);
            }
            log.debug("TestGMSClient: " + memberships.size() + " groups");
            return memberships;
        }

        @Override
        public boolean isMember(GroupURI g) {
            // not used
            return false;
        }

    }
    
    class TestTapSchemaReadAccessConverter extends TapSchemaReadAccessConverter {

        public TestTapSchemaReadAccessConverter(IdentityManager identityManager) {
            super(identityManager);
        }
        
        @Override
        boolean ensureCredentials() throws AccessControlException,
            CertificateExpiredException, CertificateNotYetValidException {
            
            return true;
        }
        
    }
}
