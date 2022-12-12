
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
import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.region.function.OracleCircle;
import ca.nrc.cadc.tap.parser.region.function.OraclePoint;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.Test;
import org.junit.Assert;


public class OracleRegionConverterTest {

    @Test
    public void handleContains() {

        final OracleRegionConverter oracleRegionConverter = new OracleRegionConverter(new ExpressionNavigator(),
                                                                                      new ReferenceNavigator(),
                                                                                      new FromItemNavigator());

        final Expression left = new OraclePoint(new Point(16.8D, 33.4D));
        final Expression right = new OracleCircle(new Circle(new Point(88.0D, 12.0D), 0.8D));
        final Expression result = oracleRegionConverter.handleContains(left, right);

        assert result instanceof Function;

        final Function resultFunction = (Function) result;

        final String resultFunctionSource = resultFunction.toString();
        Assert.assertEquals("Wrong output.",
                            "SDO_CONTAINS(SDO_UTIL.CIRCLE_POLYGON(88.0, 12.0, "
                            + 0.8D * OracleCircle.TO_METRES_ON_EARTH + ", 0.05), " +
                            "SDO_GEOMETRY(2001, 8307, SDO_POINT_TYPE(16.8, 33.4, NULL), NULL, NULL))",
                            resultFunctionSource);
    }

    @Test
    public void handleRegionPredicateContains() {
        final OracleRegionConverter oracleRegionConverter = new OracleRegionConverter(new ExpressionNavigator(),
                                                                                      new ReferenceNavigator(),
                                                                                      new FromItemNavigator());

        final Expression left = new OraclePoint(new Point(16.8D, 33.4D));
        final Expression right = new OracleCircle(new Circle(new Point(88.0D, 12.0D), 0.8D));
        final Expression containsFunction = oracleRegionConverter.handleContains(left, right);

        assert containsFunction instanceof Function;

        final BinaryExpression equals = new EqualsTo();

        equals.setLeftExpression(containsFunction);
        equals.setRightExpression(new LongValue("1"));

        final Expression result = oracleRegionConverter.handleRegionPredicate(equals);

        assert result instanceof EqualsTo;

        final EqualsTo equalsFunction = (EqualsTo) result;

        final String resultFunctionSource = equalsFunction.toString();
        Assert.assertEquals("Wrong output.",
                            "SDO_CONTAINS(SDO_UTIL.CIRCLE_POLYGON(88.0, 12.0, "
                            + 0.8D * OracleCircle.TO_METRES_ON_EARTH + ", 0.05), " +
                            "SDO_GEOMETRY"
                            + "(2001, 8307, SDO_POINT_TYPE(16.8, 33.4, NULL), NULL, NULL)) = 'TRUE'",
                            resultFunctionSource);
    }

    @Test
    public void handleColumnReferenceContains() {
        final OracleRegionConverter oracleRegionConverter = new OracleRegionConverter(new ExpressionNavigator(),
                                                                                      new ReferenceNavigator(),
                                                                                      new FromItemNavigator());

        final Expression left = new OraclePoint(new Point(88.0D, 12.0D));
        final Expression right = new Column(new Table(), "s_region");
        final Expression distanceFunction = oracleRegionConverter.handleContains(left, right);

        assert distanceFunction instanceof Function;

        Assert.assertEquals("Wrong SQL DISTANCE output.",
                            "SDO_CONTAINS(s_region, " +
                            "SDO_GEOMETRY(2001, 8307, SDO_POINT_TYPE(88.0, 12.0, NULL), NULL, NULL))",
                            distanceFunction.toString());
    }

    @Test
    public void handleColumnReferenceInside() {
        final OracleRegionConverter oracleRegionConverter = new OracleRegionConverter(new ExpressionNavigator(),
                                                                                      new ReferenceNavigator(),
                                                                                      new FromItemNavigator());

        final Expression left = new Column(new Table(), "s_region");
        final Expression right = new OraclePoint(new Point(14.0D, -2.0D));
        final Expression distanceFunction = oracleRegionConverter.handleContains(left, right);

        assert distanceFunction instanceof Function;

        Assert.assertEquals("Wrong SQL DISTANCE output.",
                            "SDO_INSIDE(s_region, " +
                            "SDO_GEOMETRY(2001, 8307, SDO_POINT_TYPE(14.0, -2.0, NULL), NULL, NULL))",
                            distanceFunction.toString());
    }

    @Test
    public void handleColumnReference() {
        final OracleRegionConverter oracleRegionConverter = new OracleRegionConverter(new ExpressionNavigator(),
                                                                                      new ReferenceNavigator(),
                                                                                      new FromItemNavigator());

        final Expression left = new OraclePoint(new Column(new Table(), "ra"),
                                                new Column(new Table(), "dec"));
        final Expression right = new OracleCircle(new Circle(new Point(88.0D, 12.0D), 0.8D));
        final Expression distanceFunction = oracleRegionConverter.handleDistance(left, right);

        assert distanceFunction instanceof Function;

        Assert.assertEquals("Wrong SQL DISTANCE output.",
                            "SDO_GEOM.SDO_DISTANCE(SDO_GEOMETRY(2001, 8307, SDO_POINT_TYPE(ra, dec, NULL), NULL, " +
                            "NULL), SDO_UTIL.CIRCLE_POLYGON(88.0, 12.0, " + 0.8D * OracleCircle.TO_METRES_ON_EARTH + ", 0.05), 0.05)",
                            distanceFunction.toString());
    }

    @Test
    public void handleRegionPredicateIntersects() {
        final OracleRegionConverter oracleRegionConverter = new OracleRegionConverter(new ExpressionNavigator(),
                                                                                      new ReferenceNavigator(),
                                                                                      new FromItemNavigator());

        final Expression left = new OraclePoint(new Point(16.8D, 33.4D));
        final Expression right = new OracleCircle(new Circle(new Point(88.0D, 12.0D), 0.8D));
        final Expression intersectsFunction = oracleRegionConverter.handleIntersects(left, right);

        assert intersectsFunction instanceof Function;

        final BinaryExpression equals = new EqualsTo();

        equals.setLeftExpression(intersectsFunction);
        equals.setRightExpression(new LongValue("0"));

        final Expression result = oracleRegionConverter.handleRegionPredicate(equals);

        assert result instanceof NotEqualsTo;

        final NotEqualsTo equalsFunction = (NotEqualsTo) result;

        final String resultFunctionSource = equalsFunction.toString();
        Assert.assertEquals("Wrong output.",
                            "SDO_GEOM.RELATE(SDO_UTIL.CIRCLE_POLYGON(88.0, 12.0, "
                            + 0.8D * OracleCircle.TO_METRES_ON_EARTH + ", 0.05), " +
                            "'anyinteract', " +
                            "SDO_GEOMETRY(2001, 8307, SDO_POINT_TYPE(16.8, 33.4, NULL), NULL, NULL), 0.05) " +
                            "<> 'TRUE'",
                            resultFunctionSource);
    }

    @Test
    public void handleColumnReferenceIntersects() {
        final OracleRegionConverter oracleRegionConverter = new OracleRegionConverter(new ExpressionNavigator(),
                                                                                      new ReferenceNavigator(),
                                                                                      new FromItemNavigator());

        final Expression left = new OraclePoint(new Point(88.0D, 12.0D));
        final Expression right = new Column(new Table("ora", "table"), "shape");
        final Expression intersectsFunction = oracleRegionConverter.handleIntersects(left, right);

        assert intersectsFunction instanceof Function;

        final BinaryExpression equals = new EqualsTo();

        equals.setLeftExpression(intersectsFunction);
        equals.setRightExpression(new LongValue("0"));

        final Expression result = oracleRegionConverter.handleRegionPredicate(equals);

        assert result instanceof NotEqualsTo;

        final NotEqualsTo equalsFunction = (NotEqualsTo) result;

        final String resultFunctionSource = equalsFunction.toString();
        Assert.assertEquals("Wrong output.",
                            "SDO_ANYINTERACT(ora.table.shape, " +
                            "SDO_GEOMETRY(2001, 8307, SDO_POINT_TYPE(88.0, 12.0, NULL), NULL, NULL)) <> 'TRUE'",
                            resultFunctionSource);
    }

    @Test
    public void handleColumnReferenceIntersectsNoRadius() {
        final double longitude = 204.25382917D;
        final double latitude = -29.86576111D;
        final double radius = 0.0d;

        final OracleRegionConverter oracleRegionConverter = new OracleRegionConverter(new ExpressionNavigator(),
                                                                                      new ReferenceNavigator(),
                                                                                      new FromItemNavigator());

        final Expression left = oracleRegionConverter.handleCircle(null,
                                                                   new DoubleValue(Double.toString(longitude)),
                                                                   new DoubleValue(Double.toString(latitude)),
                                                                   new DoubleValue(Double.toString(radius)));
        final Expression right = new Column(new Table("ora", "table"), "region");
        final Expression intersectsFunction = oracleRegionConverter.handleIntersects(left, right);

        assert intersectsFunction instanceof Function;

        final BinaryExpression equals = new EqualsTo();

        equals.setLeftExpression(intersectsFunction);
        equals.setRightExpression(new LongValue(Long.toString(0L)));

        final Expression result = oracleRegionConverter.handleRegionPredicate(equals);

        assert result instanceof NotEqualsTo;

        final NotEqualsTo equalsFunction = (NotEqualsTo) result;

        final String resultFunctionSource = equalsFunction.toString();
        Assert.assertEquals("Wrong output.",
                            "SDO_ANYINTERACT(ora.table.region, SDO_GEOMETRY(2001, 8307, "
                            + "SDO_POINT_TYPE(204.25382917, -29.86576111, NULL), NULL, NULL)) <> 'TRUE'",
                            resultFunctionSource);
    }

    @Test
    public void handleDistance() {
        final OracleRegionConverter oracleRegionConverter = new OracleRegionConverter(new ExpressionNavigator(),
                                                                                      new ReferenceNavigator(),
                                                                                      new FromItemNavigator());

        final Expression left = new OracleCircle(new Circle(new Point(88.0D, 12.0D), 0.8D));
        final Expression right = new OraclePoint(new Point(16.8D, 33.4D));
        final Expression distanceFunction = oracleRegionConverter.handleDistance(left, right);

        assert distanceFunction instanceof Function;

        Assert.assertEquals("Wrong SQL DISTANCE output.",
                            "SDO_GEOM.SDO_DISTANCE(SDO_UTIL.CIRCLE_POLYGON(88.0, 12.0, "
                            + 0.8D * OracleCircle.TO_METRES_ON_EARTH + ", 0.05), " +
                            "SDO_GEOMETRY(2001, 8307, SDO_POINT_TYPE(16.8, 33.4, NULL), NULL, NULL), 0.05)",
                            distanceFunction.toString());
    }
}
