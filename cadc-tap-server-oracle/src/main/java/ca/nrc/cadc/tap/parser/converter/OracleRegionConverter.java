
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
 *
 ************************************************************************
 */

package ca.nrc.cadc.tap.parser.converter;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import org.apache.log4j.Logger;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.stc.Box;
import ca.nrc.cadc.stc.CoordPair;
import ca.nrc.cadc.stc.Polygon;
import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.RegionFinder;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.region.function.OracleCircle;
import ca.nrc.cadc.tap.parser.region.function.OracleDistance;
import ca.nrc.cadc.tap.parser.region.function.OraclePoint;
import ca.nrc.cadc.tap.parser.region.function.OraclePolygon;

import java.util.Arrays;
import java.util.List;


public class OracleRegionConverter extends RegionFinder {
    private static final String RELATE_MASK_CONTAINS = "contains";
    private static final String RELATE_CONTAINS_TRUE_VALUE = "CONTAINS";
    private static final String RELATE_MASK_INTERSECTS = "anyinteract";
    private static final String RELATE_INTERSECTS_TRUE_VALUE = "TRUE";
    private static final String RELATE_FUNCTION_NAME = "SDO_GEOM.RELATE";
    private static final String RELATE_DEFAULT_TOLERANCE = "0.005";

    private static final Logger LOGGER = Logger.getLogger(OracleRegionConverter.class);

    public OracleRegionConverter(final ExpressionNavigator en, final ReferenceNavigator rn,
                                 final FromItemNavigator fn) {
        super(en, rn, fn);
    }

    @Override
    public Expression convertToImplementation(final Function func) {
        return super.convertToImplementation(func);
    }

    private String getRegionPredicateFunctionType(final Function function) {
        final String containsMask = String.format("'%s'", RELATE_MASK_CONTAINS);
        final String intersectsMask = String.format("'%s'", RELATE_MASK_INTERSECTS);
        for (final Object e : function.getParameters().getExpressions()) {
            if (e.toString().contains(containsMask)) {
                return RELATE_MASK_CONTAINS;
            } else if (e.toString().contains(intersectsMask)) {
                return RELATE_MASK_INTERSECTS;
            }
        }

        throw new UnsupportedOperationException(String.format("No such Region Predicate supported: %s",
                                                              function.getName()));
    }

    /**
     * This method is called when a REGION PREDICATE function is one of the arguments in a binary expression,
     * and after the direct function conversion.
     * <p>
     * Supported functions: CONTAINS, INTERSECTS
     * <p>
     * Examples:
     * <p>
     * CONTAINS() = 0
     * CONTAINS() = 1
     * 1 = CONTAINS()
     * 0 = CONTAINS()
     * <p>
     * Supported comparison operators are =, !=, &#60;, &#62;, &#60;=, &#62;=
     */
    @Override
    protected Expression handleRegionPredicate(final BinaryExpression binaryExpression) {
        LOGGER.debug("handleRegionPredicate(" + binaryExpression.getClass().getSimpleName() + "): " + binaryExpression);

        if (!(binaryExpression instanceof EqualsTo ||
            binaryExpression instanceof NotEqualsTo ||
            binaryExpression instanceof MinorThan ||
            binaryExpression instanceof GreaterThan ||
            binaryExpression instanceof MinorThanEquals ||
            binaryExpression instanceof GreaterThanEquals)) {
            return binaryExpression;
        }

        final Expression left = binaryExpression.getLeftExpression();
        final Expression right = binaryExpression.getRightExpression();

        final Function function;
        final boolean replaceLeft;
        final long value;
        if (isFunction(left) && ParserUtil.isBinaryValue(right)) {
            function = (Function) left;
            value = ((LongValue) right).getValue();
            replaceLeft = false;
        } else if (ParserUtil.isBinaryValue(left) && isFunction(right)) {
            function = (Function) right;
            value = ((LongValue) left).getValue();
            replaceLeft = true;
        } else {
            return binaryExpression;
        }

        // Should always be true, but just in case...
        if (function.getName().equals(RELATE_FUNCTION_NAME)) {
            if (!(binaryExpression instanceof EqualsTo || binaryExpression instanceof NotEqualsTo)) {
                throw new UnsupportedOperationException(
                    "Use Equals (=) or NotEquals (!=) with CONTAINS and INTERSECTS.");
            }

            final BinaryExpression returnExpression = (value == 0) ? new NotEqualsTo() : new EqualsTo();

            final String returnCompareExpressionValue =
                getRegionPredicateFunctionType(function).equals(RELATE_MASK_CONTAINS) ? RELATE_CONTAINS_TRUE_VALUE :
                    RELATE_INTERSECTS_TRUE_VALUE;
            final Expression returnCompareExpression = new StringValue(String.format("'%s'",
                                                                                     returnCompareExpressionValue));
            if (replaceLeft) {
                returnExpression.setRightExpression(right);
                returnExpression.setLeftExpression(returnCompareExpression);
            } else {
                returnExpression.setRightExpression(returnCompareExpression);
                returnExpression.setLeftExpression(left);
            }
            return returnExpression;
        } else {
            return binaryExpression;
        }
    }

    /**
     * This method is called when DISTANCE function is found.
     *
     * @param left      Left side of clause.
     * @param right     Right side of clause.
     */
    @Override
    protected Expression handleDistance(final Expression left, final Expression right) {
        return new OracleDistance(left, right, RELATE_DEFAULT_TOLERANCE);
    }

    private Expression handleRelate(final Expression left, final Expression right, final String relationMask) {
        final Function containsFunction = new Function();
        final Expression tolerance = new DoubleValue(RELATE_DEFAULT_TOLERANCE);
        final Expression containsMask = new StringValue(String.format("'%s'", relationMask));
        final ExpressionList parameters = new ExpressionList(Arrays.asList(left, containsMask, right, tolerance));

        containsFunction.setName(RELATE_FUNCTION_NAME);
        containsFunction.setParameters(parameters);

        return containsFunction;
    }

    /**
     * This method is called when a CONTAINS is found outside of a predicate.
     * This could occur if the query had CONTAINS(...) in the select list or as
     * part of an arithmetic expression or aggregate function (since CONTAINS
     * returns a numeric value).
     */
    @Override
    protected Expression handleContains(final Expression left, final Expression right) {
        return handleRelate(left, right, RELATE_MASK_CONTAINS);
    }

    /**
     * This method is called when a INTERSECTS is found outside of a predicate.
     * This could occur if the query had INTERSECTS(...) in the select list or as
     * part of an arithmetic expression or aggregate function (since INTERSECTS
     * returns a numeric value).
     */
    @Override
    protected Expression handleIntersects(Expression left, Expression right) {
        return handleRelate(left, right, RELATE_MASK_INTERSECTS);
    }

    /**
     * This method is called when a POINT geometry value is found.
     */
    @Override
    protected Expression handlePoint(Expression coordsys, Expression ra, Expression dec) {
        return new OraclePoint(ra, dec);
    }

    /**
     * This method is called when a CIRCLE geometry value is found.
     */
    @Override
    protected Expression handleCircle(Expression coordsys, Expression ra, Expression dec, Expression radius) {
        return new OracleCircle(ra, dec, radius);
    }

    /**
     * This method is called when a POLYGON geometry value is found.
     */
    @Override
    protected Expression handlePolygon(List<Expression> expressions) {
        return new OraclePolygon(expressions);
    }

    protected Expression handleRangeS2D(Expression lon1, Expression lon2, Expression lat1, Expression lat2) {
        throw new UnsupportedOperationException("RANGES2D");
    }

    /**
     * This method is called when the CENTROID function is found.
     */
    @Override
    protected Expression handleCentroid(Function adqlFunction) {
//        return new Center(adqlFunction);
        throw new UnsupportedOperationException("CENTROID");
    }

    /**
     * This method is called when COORD1 function is found.
     */
    @Override
    protected Expression handleCoord1(Function adqlFunction) {
//        return new Longitude(adqlFunction);
        throw new UnsupportedOperationException("COORD1");
    }

    /**
     * This method is called when COORD2 function is found.
     */
    @Override
    protected Expression handleCoord2(Function adqlFunction) {
//        return new Lat(adqlFunction);
        throw new UnsupportedOperationException("COORD2");
    }

    /**
     * This method is called when COORDSYS function is found.
     */
    @Override
    protected Expression handleCoordSys(Function adqlFunction) {
        return new NullValue();
    }

    boolean isFunction(Expression expression) {
        return (expression instanceof Function);
    }

    /**
     * Convert ADQL BOX to PGS spoly.
     * <p>
     * Only handle BOX() with constant parameters.
     */
    @Override
    protected Expression handleBox(Function adqlFunction) {
        Box box = ParserUtil.convertToStcBox(adqlFunction);
        Polygon stcPolygon = Polygon.getPolygon(box);
        final ca.nrc.cadc.dali.Polygon daliPolygon = new ca.nrc.cadc.dali.Polygon();

        for (final CoordPair pair : stcPolygon.getCoordPairs()) {
            daliPolygon.getVertices().add(new Point(pair.getX(), pair.getY()));
        }

        return new OraclePolygon(daliPolygon);
    }

    @Override
    protected Expression handleRegion(final Function adqlFunction) {
        return super.handleRegion(adqlFunction);
    }
}
