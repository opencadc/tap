/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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

package ca.nrc.cadc.tap.parser.converter;


import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.TapQuery;
import ca.nrc.cadc.tap.parser.TestUtil;
import ca.nrc.cadc.tap.parser.converter.postgresql.MatchConverter;
import ca.nrc.cadc.tap.parser.converter.postgresql.MatchConverterTest;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import ca.nrc.cadc.tap.parser.schema.BlobClobColumnValidator;
import ca.nrc.cadc.tap.parser.schema.ExpressionValidator;
import ca.nrc.cadc.tap.parser.schema.TapSchemaTableValidator;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
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
    
    @Test
    public void testTable() {
        try {
            String adql = "select obs_publisher_did from ivoa.ObsCore";
            String sql = doit("testSingle", "myObsCoreTable", adql);
            Assert.assertEquals("select obs_publisher_did from myObsCoreTable".toLowerCase(), sql.toLowerCase());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testSchemaTable() {
        try {
            String adql = "select obs_publisher_did from ivoa.ObsCore";
            String sql = doit("testSingle", "mySchema.myObsCoreTable", adql);
            Assert.assertEquals("select obs_publisher_did from mySchema.myObsCoreTable".toLowerCase(), sql.toLowerCase());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCatalogSchemaTable() {
        try {
            String adql = "select obs_publisher_did from ivoa.ObsCore";
            String sql = doit("testSingle", "myCatalog.mySchema.myObsCoreTable", adql);
            Assert.assertEquals("select obs_publisher_did from myCatalog.mySchema.myObsCoreTable".toLowerCase(), sql.toLowerCase());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    private String doit(String method, String tableName, String query)
    {
        try
        {
            log.info("IN: " + query);
            Parameter para = new Parameter("QUERY", query);
            job.getParameterList().add(para);
            TapQuery tapQuery = new TestQuery(tableName);
            tapQuery.setJob(job);
            String sql = tapQuery.getSQL();
            log.info(method + " OUT: " + sql);
            return sql;
        }
        finally
        {
            job.getParameterList().clear();
        }
    }
    
    Job job = new Job() 
    {
        @Override
        public String getID() { return "abcdefg"; }
    };
    
    static class TestQuery extends AdqlQuery
    {
        String internalTableName;
        TestQuery(String tn) {
            this.internalTableName = tn;
        }
        
        @Override
        protected void init()
        {
            //super.init();
            TapSchema tapSchema = TestUtil.mockTapSchema();
            TableNameConverter tnc = new TableNameConverter(true);
            tnc.put("ivoa.ObsCore", internalTableName);
            TableNameReferenceConverter tnrc = new TableNameReferenceConverter(tnc.map);
            super.navigatorList.add(new SelectNavigator(new ExpressionNavigator(), tnrc, tnc));
        }
    }
}
