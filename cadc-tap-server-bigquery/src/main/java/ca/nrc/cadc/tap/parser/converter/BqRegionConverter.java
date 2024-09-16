
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

import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.RegionFinder;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.region.function.BqCircle;
import ca.nrc.cadc.tap.parser.region.function.BqPoint;
import ca.nrc.cadc.tap.parser.region.function.BqPolygon;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BqRegionConverter extends RegionFinder {

    private static final Logger LOGGER = Logger.getLogger(BqRegionConverter.class);

    private static final String CONTAINS_FUNCTION_NAME = "ST_CONTAINS";
    private static final String NOT_FUNCTION_NAME = "NOT";
    private static final String DWITHIN_FUNCTION_NAME = "ST_DWITHIN";
    private static final String INTERSECTS_FUNCTION_NAME = "ST_INTERSECTS";
    private static final String ST_AREA_FUNCTION_NAME = "ST_AREA";
    private static final String ST_DISTANCE_FUNCTION_NAME = "ST_DISTANCE";
    private static final String ST_MAXDISTANCE_FUNCTION_NAME = "ST_MAXDISTANCE";
    private static final List<String> MEASURE_FUNCTIONS = Arrays.asList(ST_DISTANCE_FUNCTION_NAME, ST_MAXDISTANCE_FUNCTION_NAME, ST_AREA_FUNCTION_NAME);
    private static final double DEGREE_ARC_SECONDS = 3600;
    private static final double ARC_SECOND_METERS = 30.8874796235;
    private static final String SAFE_DIVIDE = "SAFE_DIVIDE";
    private static final String SAFE_MULTIPLY = "SAFE_MULTIPLY";

    public BqRegionConverter(ExpressionNavigator en, ReferenceNavigator rn,
                             FromItemNavigator fn) {
        super(en, rn, fn);
    }

    @Override
    public Expression convertToImplementation(Function func) {
        return super.convertToImplementation(func);
    }

    /**
     * This method is called when a REGION PREDICATE function is one of the arguments in a binary expression,
     * and after the direct function conversion.
     *
     * <p>Supported functions: CONTAINS
     *
     * <p>Examples:
     *
     * <p>CONTAINS() = 0
     * CONTAINS() = 1
     * 1 = CONTAINS()
     * 0 = CONTAINS()
     */
    @Override
    protected Expression handleRegionPredicate(BinaryExpression binaryExpression) {
        LOGGER.debug("handleRegionPredicate(" + binaryExpression.getClass().getSimpleName() + "): " + binaryExpression);

        binaryExpression = convertToMeters(binaryExpression);

        Expression left = binaryExpression.getLeftExpression();
        Expression right = binaryExpression.getRightExpression();

        if (!(binaryExpression instanceof EqualsTo)) {
            return binaryExpression;
        }

        Expression binaryValueExpression = null;
        Expression containsExpression = null;

        if ((isFunction(left) || isBinaryExpression(left)) && ParserUtil.isBinaryValue(right)) {
            containsExpression = left;
            binaryValueExpression = right;
        }

        if (ParserUtil.isBinaryValue(left) && (isFunction(right) || isBinaryExpression(right))) {
            containsExpression = right;
            binaryValueExpression = left;
        }

        if (containsExpression == null) {
            return binaryExpression;
        }

        boolean isTrueExpression = binaryValueExpression.toString().equals("1");
        if (!isTrueExpression) {
            Expression notBinaryExpression = handleNotExpression(containsExpression);
            return notBinaryExpression;
        }

        return containsExpression;
    }

    /**
     * This method is called when a CONTAINS is found outside of a predicate.
     * This could occur if the query had CONTAINS(...) in the select list or as
     * part of an arithmetic expression or aggregate function (since CONTAINS
     * returns a numeric value).
     * In Bq, we need to switch the arguments.
     */
    @Override
    protected Expression handleContains(Expression left, Expression right) {
        if (isCircle(left)) {
            if (isContainsPolygon(right)) {
                return polygonContainsCircle(right, left);
            }

            return circleIntersects(left, right, true);
        }

        if (isCircle(right)) {
            if (isContainsPolygon(left)) {
                return circleContainsPolygon(left, right);
            }

            return circleIntersects(left, right, true);
        }

        Function containsFunction = new Function();
        containsFunction.setName(CONTAINS_FUNCTION_NAME);
        List paramList = Arrays.asList(right, left);

        ExpressionList parameters = new ExpressionList(paramList);
        containsFunction.setParameters(parameters);

        return containsFunction;
    }

    private Function handleNotExpression(Expression containsFunction) {
        Function notFunction = new Function();
        ExpressionList parameters = new ExpressionList(Arrays.asList(containsFunction));

        notFunction.setName(NOT_FUNCTION_NAME);
        notFunction.setParameters(parameters);

        return notFunction;
    }

    /**
     * This method is called when a POINT geometry value is found.
     */
    @Override
    protected Expression handlePoint(Expression coordsys, Expression ra, Expression dec) {
        return new BqPoint(ra, dec);
    }

    /**
     * This method is called when a POLYGON geometry value is found.
     */
    @Override
    protected Expression handlePolygon(List<Expression> expressions) {
        return new BqPolygon(expressions);
    }

    /**
     * This method is called when a CIRCLE geometry value is found.
     */
    @Override
    protected Expression handleCircle(Expression coordsys, Expression ra, Expression dec, Expression radiusExpression) {

        if (isNumberValue((radiusExpression))) {
            double radius = getDoubleValue(radiusExpression).getValue();

            if (radius > 0.0D) {
                return new BqCircle(ra, dec, radiusExpression);
            }
        }

        LOGGER.debug("Radius is missing or is 0.0.  Returning a POINT instead.");
        return handlePoint(coordsys, ra, dec);
    }

    /**
     * This method is called when a INTERSECTS is found outside of a predicate.
     * This could occur if the query had INTERSECTS(...) in the select list or as
     * part of an arithmetic expression or aggregate function (since INTERSECTS
     * returns a numeric value).
     */
    @Override
    protected Expression handleIntersects(Expression left, Expression right) {
        if (isContainsCircle(left, right)) {
            return circleIntersects(left, right, false);
        }

        Function intersectsFunction = new Function();
        ExpressionList parameters = new ExpressionList(Arrays.asList(right, left));

        intersectsFunction.setName(INTERSECTS_FUNCTION_NAME);
        intersectsFunction.setParameters(parameters);

        return intersectsFunction;
    }

    @Override
    protected Expression handleArea(Function areaFunction) {
        areaFunction.setName(ST_AREA_FUNCTION_NAME);

        return metersToDegreeFunction(areaFunction);
    }

    private BinaryExpression convertToMeters(BinaryExpression binaryExpression) {
        Expression left = binaryExpression.getLeftExpression();
        Expression right = binaryExpression.getRightExpression();

        if (isFunction(right)) {
            Expression measureFunction = getInternalMeasureFunction(right);
            if (measureFunction != null) {
                right = measureFunction;
                binaryExpression.setRightExpression(right);

                if (isNumberValue(left)) {
                    DoubleValue leftNormalized = getDoubleValue(left);
                    DoubleValue leftInMeters = getArchDistanceInMetersFromExpression(leftNormalized);
                    binaryExpression.setLeftExpression(leftInMeters);
                }
            }
        }

        if (isFunction(left)) {
            Expression measureFunction = getInternalMeasureFunction(left);
            if (measureFunction != null) {
                left = measureFunction;
                binaryExpression.setLeftExpression(left);

                if (isNumberValue(right)) {
                    DoubleValue rightNormalized = getDoubleValue(right);
                    DoubleValue rightInMeters = getArchDistanceInMetersFromExpression(rightNormalized);
                    binaryExpression.setRightExpression(rightInMeters);
                }
            }
        }

        return binaryExpression;
    }

    private Expression getInternalMeasureFunction(Expression expression) {
        List<Expression> measureFunctionParams = ((Function) expression).getParameters().getExpressions();

        return measureFunctionParams
                .stream()
                .filter(exp -> isFunction(exp) && MEASURE_FUNCTIONS.contains(((Function) exp).getName()))
                .findAny()
                .orElse(null);
    }

    private Function handleDistance(Expression left, Expression right, String distanceFunctionName) {
        Function distanceFunction = new Function();
        ExpressionList parameters = new ExpressionList(Arrays.asList(right, left));

        distanceFunction.setName(distanceFunctionName);
        distanceFunction.setParameters(parameters);

        return distanceFunction;
    }

    private Expression metersToDegreeFunction(Function measuresFunction) {
        Function multiplyFunction = new Function();
        multiplyFunction.setName(SAFE_MULTIPLY);
        ExpressionList multiplyFunctionParameters = new ExpressionList(
                Arrays.asList(
                        new DoubleValue(Double.toString(DEGREE_ARC_SECONDS)),
                        new DoubleValue(Double.toString(ARC_SECOND_METERS))
                )
        );
        multiplyFunction.setParameters(multiplyFunctionParameters);

        Function divideFunction = new Function();
        divideFunction.setName(SAFE_DIVIDE);
        ExpressionList divideFunctionParameters = new ExpressionList(Arrays.asList(measuresFunction, multiplyFunction));
        divideFunction.setParameters(divideFunctionParameters);

        return divideFunction;
    }

    /**
     * This method is called when DISTANCE function is found.
     */
    @Override
    protected Expression handleDistance(Expression left, Expression right) {
        Function distanceFunction = handleDistance(left, right, ST_DISTANCE_FUNCTION_NAME);
        return metersToDegreeFunction(distanceFunction);
    }

    private DoubleValue getDoubleValue(Expression expression) {
        return isDoubleValue(expression) ?
                (DoubleValue) expression :
                new DoubleValue(((Long) ((LongValue) expression).getValue()).toString());
    }

    private boolean isDoubleValue(Expression expression) {
        return (expression instanceof DoubleValue);
    }

    private boolean isLongValue(Expression expression) {
        return (expression instanceof LongValue);
    }

    private boolean isNumberValue(Expression expression) {
        return isDoubleValue(expression) || isLongValue(expression);
    }

    private boolean isFunction(Expression expression) {
        return (expression instanceof Function);
    }

    private boolean isBinaryExpression(Expression expression) {
        return (expression instanceof BinaryExpression);
    }

    private boolean isCircle(Expression expression) {
        return expression.toString().toLowerCase().contains("circle");
    }

    private boolean isContainsCircle(Expression left, Expression right) {
        return isCircle(left) || isCircle(right);
    }

    private DoubleValue getRadius(Expression expression) {
        Expression radiusExpression = (Expression) ((Function) expression).getParameters().getExpressions().get(1);

        double radius = getDoubleValue(radiusExpression).getValue();
        double archDistanceInMeters = getArchDistanceInMeters(radius);

        return new DoubleValue(Double.toString(archDistanceInMeters));
    }

    private DoubleValue getArchDistanceInMetersFromExpression(Expression expression) {
        double measurement = ((DoubleValue) expression).getValue();
        double measurementInMeters = getArchDistanceInMeters(measurement);
        return new DoubleValue(Double.toString(measurementInMeters));
    }

    private double getArchDistanceInMeters(double measurement) {
        return measurement * DEGREE_ARC_SECONDS * ARC_SECOND_METERS;
    }

    private Expression getCircleCenterPoint(Expression expression) {
        return (Expression) ((Function) expression).getParameters().getExpressions().get(0);
    }

    private List<Expression> circleDataParams(Expression left, Expression right, boolean isContains) {
        if (isCircle(left)) {
            DoubleValue radius = right.toString().toLowerCase().contains("point") && isContains ?
                    new DoubleValue("0") :
                    getRadius(left);

            return new ArrayList<>(Arrays.asList(right, getCircleCenterPoint(left), radius));
        }

        return new ArrayList<>(Arrays.asList(getCircleCenterPoint(right), left, getRadius(right)));
    }

    private Expression shapeContainsShape(Expression left, Expression right, String distanceFunctionName, BinaryExpression binaryExpression) {
        List<Expression> paramList = circleDataParams(left, right, true);

        int lastIndex = paramList.size() - 1;
        Expression radius = paramList.get(lastIndex);
        paramList.remove(lastIndex);

        Expression containsFunction = handleDistance(paramList.get(0), paramList.get(1), distanceFunctionName);
        binaryExpression.setLeftExpression(containsFunction);
        binaryExpression.setRightExpression(radius);

        return binaryExpression;
    }

    private Expression circleContainsPolygon(Expression left, Expression right) {
        MinorThanEquals minorThanEquals = new MinorThanEquals();
        Expression containsExpression = shapeContainsShape(left, right, ST_MAXDISTANCE_FUNCTION_NAME, minorThanEquals);

        return containsExpression;
    }

    private Expression polygonContainsCircle(Expression left, Expression right) {
        GreaterThanEquals greaterThanEquals = new GreaterThanEquals();
        Expression containsExpression = shapeContainsShape(left, right, ST_DISTANCE_FUNCTION_NAME, greaterThanEquals);

        return containsExpression;
    }

    private Function circleIntersects(Expression left, Expression right, boolean isContains) {
        Function intersectsFunction = new Function();

        intersectsFunction.setName(DWITHIN_FUNCTION_NAME);
        List paramList = circleDataParams(left, right, isContains);

        ExpressionList parameters = new ExpressionList(paramList);
        intersectsFunction.setParameters(parameters);

        return intersectsFunction;
    }

    private boolean isContainsPolygon(Expression expression) {
        return expression.toString().toLowerCase().contains("polygon");
    }
}
