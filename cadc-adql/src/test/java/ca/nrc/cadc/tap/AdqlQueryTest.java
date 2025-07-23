/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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

package ca.nrc.cadc.tap;

import ca.nrc.cadc.tap.parser.TestUtil;
import ca.nrc.cadc.tap.parser.extractor.SelectListExtractor;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * A general test of AdqlQuery with no optional stuff enabled.
 * 
 * @author pdowler
 *
 */
public class AdqlQueryTest
{
    private static Logger log = Logger.getLogger(AdqlQueryTest.class);

    public String _query;
    public String _expected = "";

    SelectListExtractor _en;
    ReferenceNavigator _rn;
    FromItemNavigator _fn;
    SelectNavigator _sn;

    static TapSchema TAP_SCHEMA;

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
        TAP_SCHEMA = TestUtil.loadDefaultTapSchema();
    }

    Job job = new Job() 
    {
        @Override
        public String getID() { return "abcdefg"; }
    };
    
    private List<TapSelectItem> doit() {
        return doit(null);
    }

    private List<TapSelectItem> doit(Integer maxRowCount)
    {
        try
        {
            Parameter para = new Parameter("QUERY", _query);
            job.getParameterList().add(para);
            
            TapQuery tapQuery = new AdqlQuery();
            tapQuery.setTapSchema(TAP_SCHEMA);
            tapQuery.setJob(job);
            tapQuery.setMaxRowCount(maxRowCount);
            String sql = tapQuery.getSQL();
            List<TapSelectItem> selectList = tapQuery.getSelectList();
            log.debug("QUERY: \r\n" + _query);
            log.debug("SQL: \r\n" + sql);
            if (_expected != null) {
                Assert.assertEquals(_expected.toLowerCase().trim(), sql.toLowerCase().trim());
            }
            return selectList;
        }
        finally
        {
            job.getParameterList().clear();
        }
    }

    @Test
    public void testBasic()
    {
        _query = "select t_integer from tap_schema.alldatatypes";
        _expected = "select t_integer from tap_schema.alldatatypes";
        doit();
    }

    @Test
    public void testTableAlias()
    {
        _query = "select aa.t_integer from tap_schema.alldatatypes as aa";
        _expected = "select aa.t_integer from tap_schema.alldatatypes as aa";
        doit();
    }

    @Test
    public void testColumnAlias()
    {
        _expected = "SELECT t_complete AS xx, t_bytes AS yy FROM tap_schema.alldatatypes";
        _query = "select  t_complete as xx, t_bytes as yy from tap_schema.alldatatypes";
        doit();
    }

    @Test
    public void testSubSelectInSelect()
    {
        _query = "select schema_name as xx, (select t_integer from tap_schema.alldatatypes) from tap_schema.tables";
        _expected = "select schema_name as xx, (select t_integer from tap_schema.alldatatypes) from tap_schema.tables";
        List<TapSelectItem> selectList = doit();
        Assert.assertTrue(selectList.size() == 2);
        TapSelectItem tsi = selectList.get(1);
        Assert.assertEquals("t_integer", tsi.getName());
        Assert.assertEquals("int", tsi.getDatatype().getDatatype());
        Assert.assertEquals("int column", tsi.description);

        _query = "select schema_name as xx, (select t_varchar from tap_schema.alldatatypes) from tap_schema.tables";
        _expected = "select schema_name as xx, (select t_varchar from tap_schema.alldatatypes) from tap_schema.tables";
        selectList = doit();
        Assert.assertTrue(selectList.size() == 2);
        tsi = selectList.get(1);
        Assert.assertEquals("t_varchar", tsi.getName());
        Assert.assertEquals("char", tsi.getDatatype().getDatatype());
        Assert.assertEquals("8*", tsi.getDatatype().arraysize);
        Assert.assertEquals("varchar column", tsi.description);

        _query = "select schema_name, (select count(distinct t_bytes) from tap_schema.alldatatypes) from tap_schema.tables";
        _expected = "select schema_name, (select count(distinct t_bytes) from tap_schema.alldatatypes) from tap_schema.tables";
        selectList = doit();
        Assert.assertTrue(selectList.size() == 2);
        tsi = selectList.get(1);
        Assert.assertEquals("count2", tsi.getName().toLowerCase());
        Assert.assertEquals("long", tsi.getDatatype().getDatatype());

        _query = "select schema_name, (select count(*) from tap_schema.alldatatypes) from tap_schema.tables";
        _expected = "select schema_name, (select count(*) from tap_schema.alldatatypes) from tap_schema.tables";
        selectList = doit();
        Assert.assertTrue(selectList.size() == 2);
        tsi = selectList.get(1);
        Assert.assertEquals("count2", tsi.getName().toLowerCase());
        Assert.assertEquals("long", tsi.getDatatype().getDatatype());
    }

    //@Test
    public void testJoin()
    {
        // TODO
        doit();
    }

    //@Test
    public void testCorrelatedSubselect()
    {
        _query = "select  t_complete, aa.t_bytes, bb.* from tap_schema.alldatatypes as aa, tap_schema.tables as bb " +
                " where aa.t_complete = bb.utype " +
                "and aa.t_complete in (select utype from bb)";
        doit();
    }
    
    //@Test
    public void testUncorrelatedSubselect()
    {
        _query = "select t_complete, aa.t_bytes, bb.* from tap_schema.alldatatypes as aa, tap_schema.tables as bb " +
                " where aa.t_complete = bb.utype " +
                "and aa.t_complete in (select t_complete from tap_schema.alldatatypes)";
        doit();
    }

    @Test
    public void testTopSelect() {
        _query = "select top 25 t_integer from tap_schema.alldatatypes";
        _expected = "select top 25 t_integer from tap_schema.alldatatypes";
        doit();
    }

    @Test
    public void testLimit() {
        _query = "select t_integer from tap_schema.alldatatypes limit 25";
        //_expected = "select t_integer from tap_schema.alldatatypes limit 25";
        _expected = null;
        try {
            doit();
            Assert.fail("expected IllegalArgumentException, but successful parse");
        } catch (IllegalArgumentException expected) {
            log.info("caught expected: " + expected);
        }
    }
    
    @Test
    public void testTopMax() {
        // reduce TOP
        _query = "select top 25 t_integer from tap_schema.alldatatypes";
        _expected = "select top 10 t_integer from tap_schema.alldatatypes";
        doit(10);
        
        // retain TOP
        _query = "select top 25 t_integer from tap_schema.alldatatypes";
        _expected = "select top 25 t_integer from tap_schema.alldatatypes";
        doit(30);
        
        // inject TOP
        _query = "select t_integer from tap_schema.alldatatypes";
        _expected = "select top 10 t_integer from tap_schema.alldatatypes";
        doit(10);
    }
    
    // LIMIT currently rejected so do not need these extra tests
    //@Test
    public void testLimitMax() {
        // reduce LIMIT
        _query = "select t_integer from tap_schema.alldatatypes limit 25";
        _expected = "select t_integer from tap_schema.alldatatypes limit 10";
        doit(10);
        
        // retain LIMIT
        _query = "select t_integer from tap_schema.alldatatypes limit 25";
        _expected = "select t_integer from tap_schema.alldatatypes limit 25";
        doit(30);
        
        // inject LIMIT: injects TOP because ADQL-2.0
        //_query = "select t_integer from tap_schema.alldatatypes";
        //_expected = "select t_integer from tap_schema.alldatatypes limit 10";
        //doit(10);
    }
    
    @Test
    public void testTopAndLimit() {
        _query = "select top 10 t_integer from tap_schema.alldatatypes limit 20";
        _expected = null;
        try {
            doit();
            Assert.fail("expected IllegalArgumentException, but successful parse");
        } catch (IllegalArgumentException expected) {
            log.info("caught expected: " + expected);
        }
    }
}
