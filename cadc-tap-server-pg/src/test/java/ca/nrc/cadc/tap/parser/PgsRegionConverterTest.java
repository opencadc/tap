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

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;

import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.TapQuery;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.region.pgsphere.PgsphereRegionConverter;
import ca.nrc.cadc.tap.parser.schema.BlobClobColumnValidator;
import ca.nrc.cadc.tap.parser.schema.ExpressionValidator;
import ca.nrc.cadc.tap.parser.schema.TapSchemaTableValidator;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * test conversion of ADQL function to pgsphere implementation
 * @author Sailor Zhang
 *
 */
public class PgsRegionConverterTest
{
    private static Logger log = Logger.getLogger(PgsRegionConverterTest.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc", Level.INFO);
    }

    public String _query;
    public String _expected = "";

    Job job = new Job() 
    {
        @Override
        public String getID() { return "abcdefg"; }
    };
    
    private void doit() { doit(true); }
    
    private void doit(boolean eq)
    {
        try
        {
            Parameter para = new Parameter("QUERY", _query);
            job.getParameterList().add(para);
            log.debug("input query: " + _query);

            AdqlQuery tapQuery = new AdqlPgsRegionQuery(); // inner class in this file
            tapQuery.setJob(job);
            String sql = tapQuery.getSQL();
            log.debug("actual: " + sql);
            log.debug("expected: " + _expected);
            
            if (eq) // sql equals expected
                assertEquals(_expected.toLowerCase(), sql.toLowerCase());
            else // contains expected
                Assert.assertTrue(sql.toLowerCase().indexOf(_expected.toLowerCase()) > 0);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        finally
        {
            job.getParameterList().clear();
        }
    }

    @Test
    public void testNone()
    {
        _query = "select a, b, c from someSchema.someTable where a = b and c is not null";
        _expected = _query;
        doit();
    }

    @Test
    public void testFunctionCoordsys()
    {
        _query = "select COORDSYS(a.t_spoint) from TAP_SCHEMA.AllDataTypes";
        _expected = "SELECT NULL FROM TAP_SCHEMA.AllDataTypes";
        doit();
    }

    @Test
    public void testFunctionCoord1()
    {
        _query = "select COORD1(a.t_spoint) from TAP_SCHEMA.AllDataTypes";
        _expected = "SELECT degrees(long(a.t_spoint)) FROM TAP_SCHEMA.AllDataTypes";
        doit();
    }

    @Test
    public void testFunctionCoord2()
    {
        _query = "select COORD2(a.t_spoint) from TAP_SCHEMA.AllDataTypes";
        _expected = "SELECT degrees(lat(a.t_spoint)) FROM TAP_SCHEMA.AllDataTypes";
        doit();
    }

    @Test
    public void testIntersectsColumn()
    {
        _query = "select * from someTable where INTERSECTS(some_col, other_col) = 1";
        _expected = "select * from someTable where some_col && other_col";
        doit();
    }

    @Test
    public void testNotIntersectsColumn()
    {
        _query = "select * from someTable where INTERSECTS(some_col, other_col) = 0";
        _expected = "select * from someTable where some_col !&& other_col";
        doit();
    }

    @Test
    public void testIntersectsValue()
    {
        _query = "select * from someTable where INTERSECTS(some_col, CIRCLE('',1,2,3)) = 1";
        _expected = "select * from someTable where some_col && scircle(spoint(radians(1), radians(2)), radians(3))";
        doit();
    }

    @Test
    public void testNotIntersectsValue()
    {
        _query = "select * from someTable where INTERSECTS(some_col, CIRCLE('',1,2,3)) = 0";
        _expected = "select * from someTable where some_col !&& scircle(spoint(radians(1), radians(2)), radians(3))";
        doit();

        _query = "select * from someTable where NOT (INTERSECTS(some_col, CIRCLE('',1,2,3)) = 1)";
        _expected = "select * from someTable where not (some_col && scircle(spoint(radians(1), radians(2)), radians(3)))";
        doit();
    }

    @Test
    public void testIntersectsRegion()
    {
        _query = "select * from someTable where INTERSECTS(some_col, REGION('CIRCLE 1.0 1.0 2.0')) = 1";
        _expected = "select * from someTable where some_col && scircle(spoint(radians(1.0), radians(1.0)), radians(2.0))";
        doit();
    }

    @Test
    public void testNotIntersectsRegion()
    {
        _query = "select * from someTable where INTERSECTS(some_col, REGION('CIRCLE 1.0 1.0 2.0')) = 0";
        _expected = "select * from someTable where some_col !&& scircle(spoint(radians(1.0), radians(1.0)), radians(2.0))";
        doit();
    }

    @Test
    public void testIntersectsRegionPos()
    {
        // spoint has special handling in a predicate
        _query = "select * from someTable where INTERSECTS(some_col, REGION('POSITION 1.0 2.0')) = 1";
        _expected = "select * from someTable where some_col && cast(spoint(radians(1.0), radians(2.0)) as scircle)";
        doit();
    }

    @Test
    public void testNotIntersectsRegionPos()
    {
        // spoint has special handling in a predicate
        _query = "select * from someTable where INTERSECTS(some_col, REGION('POSITION 1.0 2.0')) = 0";
        _expected = "select * from someTable where some_col !&& cast(spoint(radians(1.0), radians(2.0)) as scircle)";
        doit();
    }

    @Test
    public void testIntersectsPoint()
    {
        // spoint has special handling in a predicate
        _query = "select * from someTable where INTERSECTS(POINT('',1,2), other_col) = 1";
        _expected = "select * from someTable where cast(spoint(radians(1), radians(2)) as scircle) && other_col";
        doit();
    }

    @Test
    public void testNotIntersectsPoint()
    {
        // spoint has special handling in a predicate
        _query = "select * from someTable where INTERSECTS(POINT('',1,2), other_col) = 0";
        _expected = "select * from someTable where cast(spoint(radians(1), radians(2)) as scircle) !&& other_col";
        doit();
    }

    @Test
    public void testContainsColumn()
    {
        _query = "select * from someTable where CONTAINS(some_col, other_col) = 1";
        _expected = "select * from someTable where some_col <@ other_col";
        doit();
    }

    @Test
    public void testNotContainsColumn()
    {
        _query = "select * from someTable where CONTAINS(some_col, other_col) = 0";
        _expected = "select * from someTable where some_col !<@ other_col";
        doit();
    }

     @Test
    public void testContainsValue()
    {
        _query = "select * from someTable where CONTAINS(CIRCLE('',1,2,3), other_col) = 1";
        _expected = "select * from someTable where scircle(spoint(radians(1), radians(2)), radians(3)) <@ other_col";
        doit();
    }

     @Test
    public void testNotContainsValue()
    {
        _query = "select * from someTable where CONTAINS(CIRCLE('',1,2,3), other_col) = 0";
        _expected = "select * from someTable where scircle(spoint(radians(1), radians(2)), radians(3)) !<@ other_col";
        doit();

        _query = "select * from someTable where NOT (CONTAINS(CIRCLE('',1,2,3), other_col) = 0)";
        _expected = "select * from someTable where not (scircle(spoint(radians(1), radians(2)), radians(3)) !<@ other_col)";
        doit();
    }

    @Test
    public void testContainsPoint()
    {
        // spoint has special handling in a predicate
        _query = "select * from someTable where CONTAINS(POINT('',1,2), other_col) = 1";
        _expected = "select * from someTable where cast(spoint(radians(1), radians(2)) as scircle) <@ other_col";
        doit();
        _query = "select * from someTable where CONTAINS(other_col, POINT('',1,2)) = 1";
        _expected = "select * from someTable where other_col <@ cast(spoint(radians(1), radians(2)) as scircle)";
        doit();
    }

    @Test
    public void testNotContainsPoint()
    {
        // spoint has special handling in a predicate
        _query = "select * from someTable where CONTAINS(POINT('',1,2), other_col) = 0";
        _expected = "select * from someTable where cast(spoint(radians(1), radians(2)) as scircle) !<@ other_col";
        doit();
    }

    // DISTANCE between a column position and a constant position
    private static final String P = "spoint(radians(ra), radians(dec))";
    private static final String Q = "spoint(radians(10), radians(20))";

    private static String lowered(String p, String q, String r, String compare)
    {
        return "(" + p + " <@ scircle(" + q + ", radians(" + r + ")) and "
            + q + " <@ scircle(" + p + ", radians(" + r + ")) and " + compare + ")";
    }

    @Test
    public void testDistanceSelect()
    {
        _query = "select DISTANCE(POINT('ICRS', ra, dec), POINT('ICRS', 10, 20)) from someTable";
        _expected = "select degrees(" + P + " <-> " + Q + ") from someTable";
        doit();
    }

    @Test
    public void testDistanceFourArgs()
    {
        _query = "select DISTANCE(ra, dec, 10, 20) from someTable";
        _expected = "select degrees(" + P + " <-> " + Q + ") from someTable";
        doit();

        _query = "select * from someTable where DISTANCE(ra, dec, 10, 20) < 1";
        _expected = "select * from someTable where " + lowered(P, Q, "1", "degrees(" + P + " <-> " + Q + ") < 1");
        doit();
    }

    @Test
    public void testDistanceUpperBound()
    {
        String d = "degrees(" + P + " <-> " + Q + ")";
        _query = "select * from someTable where DISTANCE(POINT('ICRS', ra, dec), POINT('ICRS', 10, 20)) < 0.5";
        _expected = "select * from someTable where " + lowered(P, Q, "0.5", d + " < 0.5");
        doit();

        _query = "select * from someTable where DISTANCE(POINT('ICRS', ra, dec), POINT('ICRS', 10, 20)) <= 0.5";
        _expected = "select * from someTable where " + lowered(P, Q, "0.5", d + " <= 0.5");
        doit();

        // reversed: the original comparison is kept as written for the recheck
        _query = "select * from someTable where 0.5 > DISTANCE(POINT('ICRS', ra, dec), POINT('ICRS', 10, 20))";
        _expected = "select * from someTable where " + lowered(P, Q, "0.5", "0.5 > " + d);
        doit();

        _query = "select * from someTable where 0.5 >= DISTANCE(POINT('ICRS', ra, dec), POINT('ICRS', 10, 20))";
        _expected = "select * from someTable where " + lowered(P, Q, "0.5", "0.5 >= " + d);
        doit();

        // inside a larger condition
        _query = "select * from someTable where t_integer = 1 and DISTANCE(POINT('ICRS', ra, dec), POINT('ICRS', 10, 20)) < 0.5";
        _expected = "select * from someTable where t_integer = 1 and " + lowered(P, Q, "0.5", d + " < 0.5");
        doit();
    }

    @Test
    public void testDistanceJoin()
    {
        String p = "spoint(radians(o.ra), radians(o.dec))";
        String q = "spoint(radians(u.ra), radians(u.dec))";
        _query = "select * from someTable as o join otherTable as u"
            + " on DISTANCE(POINT('ICRS', o.ra, o.dec), POINT('ICRS', u.ra, u.dec)) < 0.01";
        _expected = "select * from someTable as o join otherTable as u on "
            + lowered(p, q, "0.01", "degrees(" + p + " <-> " + q + ") < 0.01");
        doit();
    }

    @Test
    public void testDistanceConstantBound()
    {
        String d = "degrees(" + P + " <-> " + Q + ")";
        String where = "select * from someTable where ";
        String dist = "DISTANCE(POINT('ICRS', ra, dec), POINT('ICRS', 10, 20))";

        // constant expressions in [0, 30] are rewritten
        _query = where + dist + " < 30";
        _expected = where + lowered(P, Q, "30", d + " < 30");
        doit();
        _query = where + dist + " < 0";
        _expected = where + lowered(P, Q, "0", d + " < 0");
        doit();
        _query = where + dist + " < 2 * (0.5 + 1)";
        _expected = where + lowered(P, Q, "2 * (0.5 + 1)", d + " < 2 * (0.5 + 1)");
        doit();
        // integer division truncates toward zero in PostgreSQL: 61 / 2 is 30
        _query = where + dist + " < 61 / 2";
        _expected = where + lowered(P, Q, "61 / 2", d + " < 61 / 2");
        doit();
        // and -1 / 2 is 0
        _query = where + dist + " < -1 / 2";
        _expected = where + lowered(P, Q, "-1 / 2", d + " < -1 / 2");
        doit();

        // wider than getMaxIndexedRadius(), outside the scircle radius range, or not a constant: plain comparison
        String[] bounds = { "30.5", "61.0 / 2", "90", "90.5", "-0.1", "1 / 0", "t_double", "2 * t_double" };
        for (String b : bounds)
        {
            _query = where + dist + " < " + b;
            _expected = where + d + " < " + b;
            doit();
        }
    }

    @Test
    public void testDistanceNotRewritten()
    {
        String d = "degrees(" + P + " <-> " + Q + ")";
        String where = "select * from someTable where ";
        String dist = "DISTANCE(POINT('ICRS', ra, dec), POINT('ICRS', 10, 20))";

        // a lower bound, an equality, or a negation is not a containment
        String[][] cases = {
            { dist + " > 0.5", d + " > 0.5" },
            { dist + " >= 0.5", d + " >= 0.5" },
            { "0.5 < " + dist, "0.5 < " + d },
            { "0.5 <= " + dist, "0.5 <= " + d },
            { dist + " = 0.5", d + " = 0.5" },
            { dist + " <> 0.5", d + " <> 0.5" },
            { "NOT " + dist + " < 0.5", " NOT " + d + " < 0.5" }, // deparser writes a space before NOT
            // not a bare DISTANCE
            { "2 * " + dist + " < 0.5", "2 * " + d + " < 0.5" },
            { "(" + dist + ") < 0.5", "(" + d + ") < 0.5" },
        };
        for (String[] c : cases)
        {
            _query = where + c[0];
            _expected = where + c[1];
            doit();
        }
    }

    @Test
    public void testJoinAliasIntersects()
    {
        _query = "select * from someTable as t1 join otherTable as t2 on INTERSECTS(t1.someCol, t2.otherCol) = 1";
        _expected = "select * from someTable as t1 join otherTable as t2 on t1.someCol && t2.otherCol";
        doit();
    }

    @Test
    public void testPoint()
    {
        _query = "select POINT('ICRS GEOCENTER', 1, 2) from someTable";
        _expected = "spoint";
        doit(false);
    }

    @Test
    public void testCircle()
    {
        _query = "select CIRCLE('ICRS GEOCENTER', 10, 10, 1) from someTable";
        _expected = "scircle";
        doit(false);
    }

    @Test
    public void testBox()
    {
        _query = "select BOX('ICRS GEOCENTER', 10, 20, 1, 2) from someTable";
        _expected = "spoly";
        doit(false);
    }

    @Test
    public void testPolygon()
    {
        _query = "select POLYGON('ICRS GEOCENTER', 2,2,3,3,1,4) from someTable";
        _expected = "spoly";
        doit(false);
    }

    @Test
    public void testRegionBox()
    {
        _query = "select REGION('BOX ICRS GEOCENTER 11 22 10 20') from someTable";
        _expected = "spoly";
        doit(false);
    }

    @Test
    public void testRegionPolygon()
    {
        _query = "select REGION('POLYGON ICRS GEOCENTER 1 2 3 4 5 6 7 8') from someTable";
        _expected = "spoly";
        doit(false);
    }

    @Test
    public void testRegionCircle()
    {
        _query = "select REGION('CIRCLE ICRS GEOCENTER 11 22 0.5') from someTable";
        _expected = "scircle";
        doit(false);
    }

    @Test
    public void testRegionPosition()
    {
        _query = "select REGION('POSITION GALACTIC 11 22') from someTable";
        _expected = "spoint";
        doit(false);
    }
    
}

class AdqlPgsRegionQuery extends PgAdqlQuery
{
    @Override
    protected void init()
    {
        //super.init();
        TapSchema tapSchema = TestUtil.mockTapSchema();
        ExpressionNavigator en = new ExpressionValidator(tapSchema);
        ReferenceNavigator rn = new BlobClobColumnValidator(tapSchema);
        FromItemNavigator fn = new TapSchemaTableValidator(tapSchema);

        super.navigatorList.add(new PgsphereRegionConverter(en, rn, fn));
    }
}