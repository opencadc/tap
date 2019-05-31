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

/**
 * 
 */
package ca.nrc.cadc.tap.parser;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.TapQuery;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.schema.BlobClobColumnValidator;
import ca.nrc.cadc.tap.parser.schema.ExpressionValidator;
import ca.nrc.cadc.tap.parser.schema.TapSchemaTableValidator;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import junit.framework.Assert;
import org.apache.log4j.Logger;


/**
 * general test of ADQL function finder
 * 
 * @author Sailor Zhang
 *
 */
public class RegionFinderTest
{
    private static Logger log = Logger.getLogger(RegionFinderTest.class);


    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Log4jInit.setLevel("ca.nrc.cadc", org.apache.log4j.Level.INFO);
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    Job job = new Job() 
    {
        @Override
        public String getID() { return "abcdefg"; }
    };
    
    private void doit(String query)
    {
        try
        {
            log.debug("IN: " + query);
            Parameter para = new Parameter("QUERY", query);
            job.getParameterList().add(para);
            TapQuery tapQuery = new TestQuery();
            tapQuery.setJob(job);
            String sql = tapQuery.getSQL();
            //List<TapSelectItem> selectList = tapQuery.getSelectList();
            log.debug("OUT: " + sql);
            //log.debug("select-list: " + selectList);
        }
        finally
        {
            job.getParameterList().clear();
        }
    }

    @Test
    public void testAll()
    {
        String query = "select COORDSYS(a), COORD1(a), COORD2(a) from someTable as a"
            + " where 0 = CONTAINS(POINT('ICRS GEOCENTER', 25.0, -19.5), someRegionColumn) "
            + "    and INTERSECTS(a.someShape, CIRCLE('ICRS', 12, 34, 5))=1 "
            + "    and DISTANCE(a.someShape, POINT('ICRS', 12, 34)) < 1 ";
        try
        {
            doit(query);
            Assert.fail("expected UnsupportedOperationException, got nothing");
        }
        catch(UnsupportedOperationException expected)
        {

        }
        catch(Throwable unexpected)
        {
            Assert.fail("expected exception: " + unexpected);
        }
    }

    @Test
    public void testCountStar()
    {
        String query = "select count(*) from SomeTable";
        doit(query);
    }

    // TODO: re-instate fake TapSchema and test select list processing carefully

    static class TestQuery extends AdqlQuery
    {
        @Override
        protected void init()
        {
            //super.init();
            TapSchema tapSchema = TestUtil.mockTapSchema();
            ExpressionNavigator en = new ExpressionValidator(tapSchema);
            ReferenceNavigator rn = new BlobClobColumnValidator(tapSchema);
            FromItemNavigator fn = new TapSchemaTableValidator(tapSchema);
            super.navigatorList.add(new RegionFinder(en, rn, fn));
        }
    }
}
