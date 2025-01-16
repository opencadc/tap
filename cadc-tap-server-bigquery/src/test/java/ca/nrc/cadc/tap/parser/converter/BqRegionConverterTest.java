
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

import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.region.function.BqCircle;
import ca.nrc.cadc.tap.parser.region.function.BqPoint;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.Assert;
import org.junit.Test;


public class BqRegionConverterTest {

    @Test
    public void handleContains() {

        BqRegionConverter bqRegionConverter = new BqRegionConverter(new ExpressionNavigator(),
                new ReferenceNavigator(),
                new FromItemNavigator());

        Expression ra = new DoubleValue("16.8");
        Expression dec = new DoubleValue("33.4");

        Expression left = new BqPoint(ra, dec);

        Expression longitude = new DoubleValue("88.0");
        Expression latitude = new DoubleValue("12.0");
        Expression radiusInDegrees = new DoubleValue("0.8");

        Expression right = new BqCircle(longitude, latitude, radiusInDegrees);

        Expression result = bqRegionConverter.handleContains(left, right);

        assert result instanceof Function;

        Function resultFunction = (Function) result;
        String resultFunctionSource = resultFunction.toString();

        Assert.assertEquals("ST_DWITHIN(ST_GEOGPOINT(88.0, 12.0), ST_GEOGPOINT(16.8, 33.4), 88955.94131568)",
                resultFunctionSource);
    }

    @Test
    public void handleRegionPredicateContains() {
        BqRegionConverter bqRegionConverter = new BqRegionConverter(new ExpressionNavigator(),
                new ReferenceNavigator(),
                new FromItemNavigator());

        Expression ra = new DoubleValue("16.8");
        Expression dec = new DoubleValue("33.4");

        Expression left = new BqPoint(ra, dec);

        Expression longitude = new DoubleValue("88.0");
        Expression latitude = new DoubleValue("12.0");
        Expression radiusInDegrees = new DoubleValue("0.8");

        Expression right = new BqCircle(longitude, latitude, radiusInDegrees);

        Expression containsFunction = bqRegionConverter.handleContains(left, right);

        assert containsFunction instanceof Function;

        BinaryExpression equals = new EqualsTo();

        equals.setLeftExpression(containsFunction);
        equals.setRightExpression(new LongValue("1"));

        Expression result = bqRegionConverter.handleRegionPredicate(equals);
        String resultFunctionSource = result.toString();

        Assert.assertEquals("ST_DWITHIN(ST_GEOGPOINT(88.0, 12.0), ST_GEOGPOINT(16.8, 33.4), 88955.94131568)", resultFunctionSource);
    }

    @Test
    public void handleColumnReferenceContains() {
        BqRegionConverter bqRegionConverter = new BqRegionConverter(new ExpressionNavigator(),
                new ReferenceNavigator(),
                new FromItemNavigator());

        Expression ra = new DoubleValue("16.8");
        Expression dec = new DoubleValue("33.4");

        Expression left = new BqPoint(ra, dec);

        Expression right = new Column(new Table(), "s_region");
        Expression distanceFunction = bqRegionConverter.handleContains(left, right);

        assert distanceFunction instanceof Function;

        Assert.assertEquals("ST_CONTAINS(s_region, ST_GEOGPOINT(16.8, 33.4))", distanceFunction.toString());
    }

/*
    @Test
    public void handleColumnReference() {
        BqRegionConverter bqRegionConverter = new BqRegionConverter(new ExpressionNavigator(),
                new ReferenceNavigator(),
                new FromItemNavigator());

        Expression left = new BqPoint(new Column(new Table(), "ra"),
                new Column(new Table(), "dec"));

        Expression longitude = new DoubleValue("88.0");
        Expression latitude = new DoubleValue("12.0");
        Expression radiusInDegrees = new DoubleValue("0.8");

        Expression right = new BqCircle(longitude, latitude, radiusInDegrees);

        Expression distanceFunction = bqRegionConverter.handleDistance(left, right);

        assert distanceFunction instanceof Function;

        Assert.assertEquals("ST_DISTANCE(CIRCLE(ST_GEOGPOINT(88.0, 12.0), 0.8), ST_GEOGPOINT(ra, dec))", distanceFunction.toString());
    }
    */

    @Test
    public void handleRegionPredicateIntersects() {
        BqRegionConverter bqRegionConverter = new BqRegionConverter(new ExpressionNavigator(),
                new ReferenceNavigator(),
                new FromItemNavigator());

        Expression ra = new DoubleValue("16.8");
        Expression dec = new DoubleValue("33.4");

        Expression left = new BqPoint(ra, dec);

        Expression longitude = new DoubleValue("88.0");
        Expression latitude = new DoubleValue("12.0");
        Expression radiusInDegrees = new DoubleValue("0.8");

        Expression right = new BqCircle(longitude, latitude, radiusInDegrees);

        Expression intersectsFunction = bqRegionConverter.handleIntersects(left, right);

        assert intersectsFunction instanceof Function;

        BinaryExpression equals = new EqualsTo();

        equals.setLeftExpression(intersectsFunction);
        equals.setRightExpression(new LongValue("0"));

        Expression result = bqRegionConverter.handleRegionPredicate(equals);
        String resultFunctionSource = result.toString();

        Assert.assertEquals("NOT(ST_DWITHIN(ST_GEOGPOINT(88.0, 12.0), ST_GEOGPOINT(16.8, 33.4), 88955.94131568))", resultFunctionSource);
    }

    @Test
    public void handleColumnReferenceIntersects() {
        BqRegionConverter bqRegionConverter = new BqRegionConverter(new ExpressionNavigator(),
                new ReferenceNavigator(),
                new FromItemNavigator());

        Expression ra = new DoubleValue("16.8");
        Expression dec = new DoubleValue("33.4");

        Expression left = new BqPoint(ra, dec);

        Expression right = new Column(new Table("bq", "table"), "shape");

        Expression intersectsFunction = bqRegionConverter.handleIntersects(left, right);

        assert intersectsFunction instanceof Function;

        BinaryExpression equals = new EqualsTo();

        equals.setLeftExpression(intersectsFunction);
        equals.setRightExpression(new LongValue("0"));

        Expression result = bqRegionConverter.handleRegionPredicate(equals);
        String resultFunctionSource = result.toString();

        Assert.assertEquals("NOT(ST_INTERSECTS(bq.table.shape, ST_GEOGPOINT(16.8, 33.4)))", resultFunctionSource);
    }

    @Test
    public void handleColumnReferenceIntersectsNoRadius() {
        final double longitude = 204.25382917D;
        final double latitude = -29.86576111D;
        final double radius = 0.0d;

        BqRegionConverter bqRegionConverter = new BqRegionConverter(new ExpressionNavigator(),
                new ReferenceNavigator(),
                new FromItemNavigator());

        Expression left = bqRegionConverter.handleCircle(null,
                new DoubleValue(Double.toString(longitude)),
                new DoubleValue(Double.toString(latitude)),
                new DoubleValue(Double.toString(radius)));
        Expression right = new Column(new Table("bq", "table"), "region");
        Expression intersectsFunction = bqRegionConverter.handleIntersects(left, right);

        assert intersectsFunction instanceof Function;

        BinaryExpression equals = new EqualsTo();

        equals.setLeftExpression(intersectsFunction);
        equals.setRightExpression(new LongValue(Long.toString(0L)));

        Expression result = bqRegionConverter.handleRegionPredicate(equals);
        String resultFunctionSource = result.toString();

        Assert.assertEquals("NOT(ST_INTERSECTS(bq.table.region, ST_GEOGPOINT(204.25382917, -29.86576111)))", resultFunctionSource);
    }

    
    @Test
    public void handleDistance() {
        BqRegionConverter bqRegionConverter = new BqRegionConverter(new ExpressionNavigator(),
                new ReferenceNavigator(),
                new FromItemNavigator());

        Expression longitude = new DoubleValue("88.0");
        Expression latitude = new DoubleValue("12.0");
        Expression radiusInDegrees = new DoubleValue("0.8");

        Expression left = new BqCircle(longitude, latitude, radiusInDegrees);

        Expression ra = new DoubleValue("16.8");
        Expression dec = new DoubleValue("33.4");

        Expression right = new BqPoint(ra, dec);

        Expression distanceFunction = bqRegionConverter.handleDistance(left, right);

        assert distanceFunction instanceof Function;

        Assert.assertEquals("SAFE_DIVIDE(ST_DISTANCE(ST_GEOGPOINT(16.8, 33.4), CIRCLE(ST_GEOGPOINT(88.0, 12.0), 0.8)), SAFE_MULTIPLY(3600.0, 30.8874796235))", distanceFunction.toString());
    }
    
    
}
