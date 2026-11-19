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
*  $Revision: 1 $
*
************************************************************************
*/

package ca.nrc.cadc.tap.parser.region.pgsphere;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;

import org.apache.log4j.Logger;

import ca.nrc.cadc.stc.Box;
import ca.nrc.cadc.stc.Circle;
import ca.nrc.cadc.stc.Polygon;
import ca.nrc.cadc.stc.Position;
import ca.nrc.cadc.stc.STC;
import ca.nrc.cadc.stc.StcsParsingException;
import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.RegionFinder;
import ca.nrc.cadc.tap.parser.function.Operator;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Center;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Distance;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Lat;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Longitude;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Sbox;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Scircle;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Spoint;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Spoly;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

/**
 * Convert ADQL functions into PgSphere implementation.
 *
 * @author pdowler, zhangsa
 */
public class PgsphereRegionConverter extends RegionFinder
{
    @SuppressWarnings("unused")
    private static Logger log = Logger.getLogger(PgsphereRegionConverter.class);
    
    /**
     * Prototype coordinate range function a la sbox in pgsphere.
     */
    public static final String RANGE_S2D = "RANGE_S2D";

    public PgsphereRegionConverter(ExpressionNavigator en, ReferenceNavigator rn, FromItemNavigator fn)
    {
        super(en, rn, fn);
    }
    
    @Override
    public Expression convertToImplementation(Function func)
    {
        Expression implExpr = super.convertToImplementation(func);
        if (implExpr == func) // not handled
        {
            if (RANGE_S2D.equalsIgnoreCase(func.getName()))
            {
                ExpressionList exprList = func.getParameters();
                if (exprList == null)
                    throw new IllegalArgumentException("RANGE_S2D requires long1, long2, lat1, lat2");
                List<Expression> expressions = exprList.getExpressions();
                if (expressions.size() != 4)
                    throw new IllegalArgumentException("RANGE_S2D requires long1, long2, lat1, lat2");
                implExpr = handleRangeS2D(expressions.get(0), expressions.get(1), expressions.get(2), expressions.get(3));
            }
        }
        return implExpr;
    }

    /**
     * This method is called when a REGION PREDICATE function is one of the arguments in a binary expression,
     * and after the direct function conversion.
     *
     * Supported functions: CONTAINS, INTERSECTS
     *
     * Examples:
     *
     * CONTAINS() = 0
     * CONTAINS() = 1
     * 1 = CONTAINS()
     * 0 = CONTAINS()
     *
     * Supported comparison operators are =, !=, &#60;, &#62;, &#60;=, &#62;=
     */
    @Override
    protected Expression handleRegionPredicate(BinaryExpression binaryExpression)
    {
        log.debug("handleRegionPredicate(" + binaryExpression.getClass().getSimpleName() + "): " + binaryExpression);

        Expression distancePredicate = handleDistancePredicate(binaryExpression);
        if (distancePredicate != null)
            return distancePredicate;

        if (!(binaryExpression instanceof EqualsTo ||
            binaryExpression instanceof NotEqualsTo ||
            binaryExpression instanceof MinorThan ||
            binaryExpression instanceof GreaterThan ||
            binaryExpression instanceof MinorThanEquals ||
            binaryExpression instanceof GreaterThanEquals))
            return binaryExpression;

        Expression left = binaryExpression.getLeftExpression();
        Expression right = binaryExpression.getRightExpression();

        Operator operator = null;
        long value = 0;
        if (isOperator(left) && ParserUtil.isBinaryValue(right))
        {
            operator = (Operator) left;
            value = ((LongValue) right).getValue();
        }
        else if (ParserUtil.isBinaryValue(left) && isOperator(right))
        {
            operator = (Operator) right;
            value = ((LongValue) left).getValue();
        }
        else
        {
            return binaryExpression;
        }

        if (value == 0)
            operator.negate();
        return operator;
    }

    /**
     * This method is called when a CONTAINS is found outside of a predicate.
     * This could occur if the query had CONTAINS(...) in the select list or as
     * part of an arithmetic expression or aggregate function (since CONTAINS
     * returns a numeric value).
     *
     */
    @Override
    protected Expression handleContains(Expression left, Expression right)
    {
        return new Operator("<@", "!<@", left, right);
    }

    /**
     * This method is called when a INTERSECTS is found outside of a predicate.
     * This could occur if the query had INTERSECTS(...) in the select list or as
     * part of an arithmetic expression or aggregate function (since INTERSECTS
     * returns a numeric value).
     *
     */
    @Override
    protected Expression handleIntersects(Expression left, Expression right)
    {
        return new Operator("&&", "!&&", left, right);
    }

    /**
     * This method is called when DISTANCE function is found.
     *
     */
    @Override
    protected Expression handleDistance(Expression left, Expression right)
    {
        return new Distance(left, right);
    }

    /**
     * Rewrite an upper bound on DISTANCE into a form a GiST index on the point
     * expression can serve. degrees(p &lt;-&gt; q) &lt; r is a computed value, so
     * every such query (cone search, positional cross-match) scans the table.
     *
     * <p>DISTANCE(p, q) &lt; r, &lt;= r and the reversed r &gt; DISTANCE(p, q),
     * r &gt;= DISTANCE(p, q) become
     * <pre>
     * (p &lt;@ scircle(q, radians(r)) AND q &lt;@ scircle(p, radians(r)) AND degrees(p &lt;-&gt; q) &lt; r)
     * </pre>
     * <ul>
     * <li>&lt;@ includes the circle boundary, so each containment term is a
     * superset of both &lt; and &lt;=: a filter, not the answer.</li>
     * <li>the mirror term is redundant for the result but not for the planner:
     * only the left operand of &lt;@ can match an index on the point expression,
     * so both orders are emitted and the planner uses whichever side is indexed.</li>
     * <li>the original comparison is kept unchanged as the exact recheck, so rows
     * at exactly distance r are kept or dropped as before.</li>
     * </ul>
     *
     * <p>Only a bare DISTANCE on one side and a constant bound from 0 to
     * getMaxIndexedRadius() degrees are rewritten. Anything else (&gt;, =, NOT,
     * 2*DISTANCE(...), a per-row radius, a wide cone) returns null and keeps the
     * plain degrees(p &lt;-&gt; q) comparison.
     *
     * @param binaryExpression comparison whose arguments have already been converted
     * @return rewritten predicate, or null if this is not a shape that rewrites exactly
     */
    protected Expression handleDistancePredicate(BinaryExpression binaryExpression)
    {
        if (binaryExpression.isNot())
            return null;

        Distance distance;
        Expression bound;
        if ((binaryExpression instanceof MinorThan || binaryExpression instanceof MinorThanEquals)
            && binaryExpression.getLeftExpression() instanceof Distance)
        {
            distance = (Distance) binaryExpression.getLeftExpression();
            bound = binaryExpression.getRightExpression();
        }
        else if ((binaryExpression instanceof GreaterThan || binaryExpression instanceof GreaterThanEquals)
            && binaryExpression.getRightExpression() instanceof Distance)
        {
            distance = (Distance) binaryExpression.getRightExpression();
            bound = binaryExpression.getLeftExpression();
        }
        else
            return null;

        // pgsphere rejects an scircle radius outside [0, 90] degrees: a bound outside it
        // would turn a query that ran into one that fails
        Constant radius = constantValue(bound);
        double max = Math.min(90.0, getMaxIndexedRadius());
        if (radius == null || radius.value.signum() < 0 || radius.value.compareTo(BigDecimal.valueOf(max)) > 0)
            return null;

        Expression p = distance.getLeft();
        Expression q = distance.getRight();
        Expression pInQ = Distance.pointOperator("<@", p, scircle(q, bound));
        Expression qInP = Distance.pointOperator("<@", q, scircle(p, bound));
        return new Parenthesis(new AndExpression(new AndExpression(pInQ, qInP), binaryExpression));
    }

    /**
     * Largest DISTANCE bound, in degrees, that handleDistancePredicate rewrites to
     * use an index. A wide cone selects a large fraction of the table, where a
     * sequential scan is faster; and the planner, multiplying the selectivities of
     * the three terms, estimates very few rows and so prefers the index. On 500k
     * uniformly distributed rows the rewrite is faster up to 45 degrees and slower
     * from about 60; the default leaves a margin for less uniform skies. At most 90
     * (the scircle radius limit).
     *
     * @return maximum radius in degrees
     */
    protected double getMaxIndexedRadius()
    {
        return 30.0;
    }

    // scircle(center, radians(radius)) with radius in degrees
    private static Function scircle(Expression center, Expression radius)
    {
        List<Expression> radiusExp = new ArrayList<Expression>();
        radiusExp.add(radius);
        Function radiusFunc = new Function();
        radiusFunc.setName("radians");
        radiusFunc.setParameters(new ExpressionList(radiusExp));

        List<Expression> expressions = new ArrayList<Expression>();
        expressions.add(center);
        expressions.add(radiusFunc);
        Function ret = new Function();
        ret.setName("scircle");
        ret.setParameters(new ExpressionList(expressions));
        return ret;
    }

    // value of a numeric constant and whether PostgreSQL types it as an integer
    private static class Constant
    {
        final BigDecimal value;
        final boolean integer;

        Constant(BigDecimal value, boolean integer)
        {
            this.value = value;
            this.integer = integer;
        }
    }

    /**
     * The value PostgreSQL computes for a constant expression, or null if the
     * expression is not a constant this method understands: numeric literals,
     * unary minus, parentheses, and + - * /. The range check has to see the number
     * PostgreSQL will compute from the SQL as emitted: decimal literals are exact
     * numeric, and / between two integers truncates toward zero.
     */
    private static Constant constantValue(Expression e)
    {
        if (e instanceof LongValue)
            return new Constant(new BigDecimal(((LongValue) e).getStringValue()), true);
        if (e instanceof DoubleValue)
        {
            // the deparser writes getValue(), not the text the user typed
            return new Constant(new BigDecimal(Double.toString(((DoubleValue) e).getValue())), false);
        }
        if (e instanceof Parenthesis)
        {
            Parenthesis par = (Parenthesis) e;
            return par.isNot() ? null : constantValue(par.getExpression());
        }
        if (e instanceof InverseExpression)
        {
            Constant c = constantValue(((InverseExpression) e).getExpression());
            return c == null ? null : new Constant(c.value.negate(), c.integer);
        }
        if (e instanceof Addition || e instanceof Subtraction || e instanceof Multiplication || e instanceof Division)
        {
            BinaryExpression be = (BinaryExpression) e;
            if (be.isNot())
                return null;
            Constant a = constantValue(be.getLeftExpression());
            Constant b = constantValue(be.getRightExpression());
            if (a == null || b == null)
                return null;
            boolean integer = a.integer && b.integer;
            if (e instanceof Addition)
                return new Constant(a.value.add(b.value), integer);
            if (e instanceof Subtraction)
                return new Constant(a.value.subtract(b.value), integer);
            if (e instanceof Multiplication)
                return new Constant(a.value.multiply(b.value), integer);
            if (b.value.signum() == 0)
                return null;
            if (integer) // BigInteger division truncates toward zero, as PostgreSQL integer division does
                return new Constant(new BigDecimal(a.value.toBigInteger().divide(b.value.toBigInteger())), true);
            return new Constant(a.value.divide(b.value, MathContext.DECIMAL128), false);
        }
        return null;
    }

    /**
     * This method is called when a POINT geometry value is found.
     *
     */
    @Override
    protected Expression handlePoint(Expression coordsys, Expression ra, Expression dec)
    {
        return new Spoint(coordsys, ra, dec);
    }

    /**
     * This method is called when a CIRCLE geometry value is found.
     *
     */
    @Override
    protected Expression handleCircle(Expression coordsys, Expression ra, Expression dec, Expression radius)
    {
        return new Scircle(coordsys, ra, dec, radius);
    }

    /**
     * This method is called when a POLYGON geometry value is found.
     *
     */
    @Override
    protected Expression handlePolygon(List<Expression> expressions)
    {
        return new Spoly(expressions);
    }
    
    protected Expression handleRangeS2D(Expression lon1, Expression lon2, Expression lat1, Expression lat2)
    {
        return new Sbox(lon1, lon2, lat1, lat2);
    }

    /**
     * This method is called when the CENTROID function is found.
     *
     */
    @Override
    protected Expression handleCentroid(Function adqlFunction)
    {
        return new Center(adqlFunction);
    }

    /**
     * This method is called when COORD1 function is found.
     *
     */
    @Override
    protected Expression handleCoord1(Function adqlFunction)
    {
        return new Longitude(adqlFunction);
    }

    /**
     * This method is called when COORD2 function is found.
     *
     */
    @Override
    protected Expression handleCoord2(Function adqlFunction)
    {
        return new Lat(adqlFunction);
    }

    /**
     * This method is called when COORDSYS function is found.
     */
    @Override
    protected Expression handleCoordSys(Function adqlFunction)
    {
        return new NullValue();
    }

    protected boolean isOperator(Expression expression)
    {
        return (expression instanceof Operator);
    }
    /**
     * Convert ADQL BOX to PGS spoly.
     *
     * Only handle BOX() with constant parameters.
     *
     */
    @Override
    protected Expression handleBox(Function adqlFunction)
    {
        Box box = ParserUtil.convertToStcBox(adqlFunction);
        Polygon polygon = Polygon.getPolygon(box);
        return new Spoly(polygon);
    }

    @Override
    protected Expression handleRegion(Function adqlFunction)
    {
        List<Expression> params = adqlFunction.getParameters().getExpressions();
        StringValue strV = (StringValue) params.get(0);
        String regionParamStr = strV.getValue();
        String[] tokens = regionParamStr.split(" ");
        String fname = tokens[0].toUpperCase();

        if (Box.NAME.equalsIgnoreCase(fname))
        {
            Box box;
            try
            {
                box = (Box) STC.parse(regionParamStr);
            }
            catch (StcsParsingException e)
            {
                throw new IllegalArgumentException(e);
            }
            Polygon polygon = Polygon.getPolygon(box);
            return new Spoly(polygon);
        }
        else if (Polygon.NAME.equalsIgnoreCase(fname))
        {
            Polygon polygon;
            try
            {
                polygon = (Polygon) STC.parse(regionParamStr);
            }
            catch (StcsParsingException e)
            {
                throw new IllegalArgumentException(e);
            }
            return new Spoly(polygon);
        }
        else if (Circle.NAME.equalsIgnoreCase(fname))
        {
            Circle circle;
            try
            {
                circle = (Circle) STC.parse(regionParamStr);
            }
            catch (StcsParsingException e)
            {
                throw new IllegalArgumentException(e);
            }
            return new Scircle(circle);
        }
        else if (Position.NAME.equalsIgnoreCase(fname))
        {
            Position position;
            try
            {
                position = (Position) STC.parse(regionParamStr);
            }
            catch (StcsParsingException e)
            {
                throw new IllegalArgumentException(e);
            }
            return new Spoint(position);
        }
        else
        {
            return super.handleRegion(adqlFunction);
        }

    }
}
