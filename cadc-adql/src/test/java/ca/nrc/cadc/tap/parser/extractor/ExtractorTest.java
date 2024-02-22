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

package ca.nrc.cadc.tap.parser.extractor;

import ca.nrc.cadc.tap.TapSelectItem;
import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.TestUtil;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.Statement;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.FunctionDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

/**
 * test the extraction of select list
 * 
 * @author Sailor Zhang
 *
 */
public class ExtractorTest
{
    private static final Logger log = Logger.getLogger(ExtractorTest.class);

    private static final String ADQL_DOUBLE = "adql:DOUBLE";
    private static final String ADQL_VARCHAR = "adql:VARCHAR";

    SelectListExpressionExtractor _en;
    ReferenceNavigator _rn;
    FromItemNavigator _fn;
    SelectNavigator _sn;

    static TapSchema TAP_SCHEMA;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
        TAP_SCHEMA = TestUtil.loadDefaultTapSchema();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        _en = new SelectListExpressionExtractor(TAP_SCHEMA);
        _rn = new ReferenceNavigator();
        _fn = new FromItemNavigator();
        _sn = new SelectListExtractor(_en, _rn, _fn);
    }

    private void doit(String query, List<TapSelectItem> expectedList)
    {
        boolean isValidQuery = true;
        doit(query, expectedList, isValidQuery);
    }

    private void doit(String query, List<TapSelectItem> expectedList, boolean isValidQuery)
    {
        log.debug("query: "  + query);
        Statement s = null;
        try
        {
            boolean exceptionThrown = false;
            s = ParserUtil.receiveQuery(query);
            try
            {
                ParserUtil.parseStatement(s, _sn);
                log.debug("statement: " + s);
                List<TapSelectItem> selectList = _en.getSelectList();
                for (int i = 0; i < expectedList.size(); i++)
                {
                    TapSelectItem expected = expectedList.get(i);
                    TapSelectItem actual = selectList.get(i);
                    log.debug("expected: " + expected);
                    log.debug("actual: " + actual);

                    Assert.assertEquals(expected.getName(), actual.getName());
                    Assert.assertEquals(expected.description, actual.description);
                    Assert.assertEquals(expected.getDatatype(), actual.getDatatype());
                    Assert.assertEquals(expected.utype, actual.utype);
                    Assert.assertEquals(expected.unit, actual.unit);
                    Assert.assertEquals(expected.ucd, actual.ucd);
                }
            }
            catch (Exception e)
            {
                exceptionThrown = true;
                if (!isValidQuery)
                    log.debug("expected exception: " + e.getMessage());
                else
                    throw e;
            }
            Assert.assertTrue(isValidQuery != exceptionThrown);
        } 
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAll()
    {
        String query = "select t.table_name as tn, keys.from_table from tap_schema.tables t, tap_schema.keys where t.utype=keys.utype";

        List<TapSelectItem> expectedList = new ArrayList<>();
        ColumnDesc columnDesc = new ColumnDesc("tables", "table_name", new TapDataType("char", "16*", null));
        TapSelectItem paramDesc = new TapSelectItem("tn", columnDesc);
        expectedList.add(paramDesc);
        columnDesc = new ColumnDesc("keys", "from_table", new TapDataType("char", "16*", null));
        paramDesc = new TapSelectItem(columnDesc.getColumnName(), columnDesc);
        expectedList.add(paramDesc);
        
        doit(query, expectedList);
    }

    @Test
    public void testQuotedCol()
    {
        String query = "select \"table_name\" from tap_schema.tables";

        List<TapSelectItem> expectedList = new ArrayList<>();
        ColumnDesc columnDesc = new ColumnDesc("tables", "table_name", new TapDataType("char", "16*", null));
        TapSelectItem paramDesc = new TapSelectItem("\"table_name\"", columnDesc);
        expectedList.add(paramDesc);

        doit(query, expectedList);
    }

    @Test
    public void testColumnExpression()
    {
        String query = "select position_center_ra from caom.siav1";

        List<TapSelectItem> expectedList = new ArrayList<>();
        ColumnDesc columnDesc = new ColumnDesc("siav1", "position_center_ra", TapDataType.DOUBLE);
        TapSelectItem paramDesc = new TapSelectItem(columnDesc.getColumnName(), columnDesc);
        expectedList.add(paramDesc);

        doit(query, expectedList);
    }

    @Test
    public void testFunctionExpression()
    {
        String query = "select area(position_center_ra) from caom.siav1";

        List<TapSelectItem> expectedList = new ArrayList<>();
        TapSelectItem paramDesc = new TapSelectItem("area1", TapDataType.DOUBLE);
        expectedList.add(paramDesc);
        
        doit(query, expectedList);
    }

    @Test
    public void testAllColumnmsExpression()
    {
        boolean isValidQuery = false;
        String query = "select * from caom.siav1";
        doit(query, null, isValidQuery);
    }

    @Test
    public void testAllTableColumnmsExpression()
    {
        boolean isValidQuery = false;
        String query = "select siav1.* from caom.siav1";
        doit(query, null, isValidQuery);
    }

    @Test
    public void testArgumentDatatypeFunctionExpression()
    {
        String query = "select max(t_double), max(t_long) from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<>();
        TapSelectItem p1 = new TapSelectItem("max1", TapDataType.DOUBLE);
        TapSelectItem p2 = new TapSelectItem("max2", TapDataType.LONG);
        expectedList.add(p1);
        expectedList.add(p2);

        doit(query, expectedList);
    }

    @Test
    public void testNestedFunctionExpression()
    {
        String query = "select max(area(position_center_ra)) from caom.siav1";

        List<TapSelectItem> expectedList = new ArrayList<>();
        TapSelectItem paramDesc = new TapSelectItem("max1", TapDataType.DOUBLE);
        expectedList.add(paramDesc);

        doit(query, expectedList);
    }

    @Test
    public void testNestedFunctionExpression2()
    {
        String query = "select area(max(position_center_ra)) from caom.siav1";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem paramDesc = new TapSelectItem("area1", TapDataType.DOUBLE);
        expectedList.add(paramDesc);

        doit(query, expectedList);
    }

    @Test
    public void testRecursiveNestedFunctionExpression()
    {
        String query = "select max(min(avg(position_center_ra))) from caom.siav1";

        List<TapSelectItem> expectedList = new ArrayList<>();
        TapSelectItem paramDesc = new TapSelectItem("max1", TapDataType.DOUBLE);
        expectedList.add(paramDesc);

        doit(query, expectedList);
    }

    @Test
    public void testConstantExpression()
    {
        String query = "select 1, 0.999 from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<>();
        TapSelectItem p1 = new TapSelectItem("col1", new TapDataType("long", null, null));
        TapSelectItem p2 = new TapSelectItem("col2", new TapDataType("double", null, null));
        expectedList.add(p1);
        expectedList.add(p2);

        doit(query, expectedList);
    }

    @Test
    public void testConstantWithAliasExpression()
    {
        String query = "select 1 as one, 0.5 as half from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("one", new TapDataType("long", null, null));
        TapSelectItem p2 = new TapSelectItem("half", new TapDataType("double", null, null));
        expectedList.add(p1);
        expectedList.add(p2);

        doit(query, expectedList);
    }

    @Test
    public void testSubselectExpression()
    {
        String query = "select (select 1 from tap_schema.alldatatypes) from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("col1", new TapDataType("long", null, null));
        expectedList.add(p1);

        doit(query, expectedList);
    }

    @Test
    public void testSubselectWithAliasExpression()
    {
        String query = "select (select 1 as one from tap_schema.alldatatypes), 0.5 as half from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("one", new TapDataType("long", null, null));
        TapSelectItem p2 = new TapSelectItem("half", new TapDataType("double", null, null));
        expectedList.add(p1);
        expectedList.add(p2);

        doit(query, expectedList);
    }

    @Test
    public void testFunctionWithConstantExpression()
    {
        String query = "select max(1), max(0.5) as foo from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("max1", new TapDataType("long", null, null));
        TapSelectItem p2 = new TapSelectItem("foo", new TapDataType("double", null, null));
        expectedList.add(p1);
        expectedList.add(p2);

        doit(query, expectedList);
    }

    @Test
    public void testQueryWithThreeFunctions()
    {
        String query = "select max(1), min(1.0), count(5) from caom.siav1";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("max1", new TapDataType("long", null, null));
        TapSelectItem p2 = new TapSelectItem("min2", new TapDataType("double", null, null));
        TapSelectItem p3 = new TapSelectItem("count3", new TapDataType("long", null, null));
        expectedList.add(p1);
        expectedList.add(p2);
        expectedList.add(p3);

        doit(query, expectedList);
    }

    @Test
    public void testQueryWithMath()
    {
        String query = "select 2+2, 2-2 as zero, 2*2, 2/2, 1.5+1.5 from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("col1", new TapDataType("long", null, null));
        TapSelectItem p2 = new TapSelectItem("zero", new TapDataType("long", null, null));
        TapSelectItem p3 = new TapSelectItem("col3", new TapDataType("long", null, null));
        TapSelectItem p4 = new TapSelectItem("col4", new TapDataType("double", null, null));
        TapSelectItem p5 = new TapSelectItem("col5", new TapDataType("double", null, null));
        expectedList.add(p1);
        expectedList.add(p2);
        expectedList.add(p3);
        expectedList.add(p4);
        expectedList.add(p5);

        doit(query, expectedList);
    }

    @Test
    public void testQueryWithLongMath()
    {
        String query = "select 1+t_long, 0-t_long, 2*t_long, 1.5*t_long, t_long/5 from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("col1", new TapDataType("long", null, null));
        TapSelectItem p2 = new TapSelectItem("col2", new TapDataType("long", null, null));
        TapSelectItem p3 = new TapSelectItem("col3", new TapDataType("long", null, null));
        TapSelectItem p4 = new TapSelectItem("col4", new TapDataType("double", null, null));
        TapSelectItem p5 = new TapSelectItem("col5", new TapDataType("double", null, null));
        expectedList.add(p1);
        expectedList.add(p2);
        expectedList.add(p3);
        expectedList.add(p4);
        expectedList.add(p5);

        doit(query, expectedList);
    }

    @Test
    public void testQueryWithMathColumns()
    {
        String query = "select t_integer + t_integer, t_double + t_integer from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("col1", new TapDataType("long", null, null));
        TapSelectItem p2 = new TapSelectItem("col2", new TapDataType("double", null, null));
        expectedList.add(p1);
        expectedList.add(p2);

        doit(query, expectedList);
    }

    @Test
    public void testQueryWithMathFunctions()
    {
        String query = "select (max(t_double) - min(t_long))/count(t_long) from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("col1", new TapDataType("double", null, null));
        expectedList.add(p1);

        doit(query, expectedList);
    }

    @Test
    public void testQueryWithLongCase()
    {
        String query = "select sum(case when t_long < 2 then 1 else 0 end) as math from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("math", new TapDataType("long", null, null));
        expectedList.add(p1);

        doit(query, expectedList);
    }

    @Test
    public void testQueryWithDoubleCase()
    {
        String query = "select case when t_long < 2 then t_double else 0 end from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("col1", new TapDataType("double", null, null));
        expectedList.add(p1);

        doit(query, expectedList);
    }

    @Test
    public void testQueryWithStringConstantCase()
    {
        String query = "select case when t_double < 2 then 'abc' else 0.0 end from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("col1", new TapDataType("char", "*", null));
        expectedList.add(p1);

        doit(query, expectedList);
    }

    @Test
    public void testQueryWithStringElseCase()
    {
        String query = "select (case when t_double < 2 then 0.0 else 'abc' end) from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("col1", new TapDataType("char", "*", null));
        expectedList.add(p1);

        doit(query, expectedList);
    }

    @Test
    public void testQueryWithStringColumnElseCase()
    {
        String query = "select (case when t_double < 2 then 0 else t_char end) from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("col1", new TapDataType("char", "*", null));
        expectedList.add(p1);

        doit(query, expectedList);
    }

    @Test
    public void testQueryWithIntColumnElseCase()
    {
        String query = "select (case when t_double < 2 then t_integer else t_integer end) from tap_schema.alldatatypes";

        List<TapSelectItem> expectedList = new ArrayList<TapSelectItem>();
        TapSelectItem p1 = new TapSelectItem("col1", new TapDataType("long", null, null));
        expectedList.add(p1);

        doit(query, expectedList);
    }
}
