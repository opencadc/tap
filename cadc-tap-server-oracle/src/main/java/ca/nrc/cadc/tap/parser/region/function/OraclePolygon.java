
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

package ca.nrc.cadc.tap.parser.region.function;

import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.tap.parser.RegionFinder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;


public class OraclePolygon extends OracleGeometricFunction {

    static final int ORACLE_POLYGON_ELEMENT_INFO_TYPE = 1003;

    private static final int[] ORACLE_ELEMENT_INFO_VALUES = new int[] {
        1, ORACLE_POLYGON_ELEMENT_INFO_TYPE, 1
    };

    // Outer Polygon element
    private static final Expression[] ORACLE_ELEMENT_INFO = new Expression[ORACLE_ELEMENT_INFO_VALUES.length];

    static {
        for (int i = 0; i < ORACLE_ELEMENT_INFO_VALUES.length; i++) {
            ORACLE_ELEMENT_INFO[i] = new LongValue(Long.toString(ORACLE_ELEMENT_INFO_VALUES[i]));
        }
    }

    private final List<Expression> vertices = new ArrayList<>();


    private OraclePolygon() {
        super(ORACLE_ELEMENT_INFO);
    }

    public OraclePolygon(final List<Expression> verticeExpressions) {
        this();
        if (verticeExpressions != null) {
            this.vertices.addAll(verticeExpressions);
        }
        processOrdinateParameters();
    }

    public OraclePolygon(final Polygon polygon) {
        this();
        vertices.add(new StringValue(RegionFinder.ICRS));
        for (final Point p : polygon.getVertices()) {
            vertices.add(new DoubleValue(Double.toString(p.getLongitude())));
            vertices.add(new DoubleValue(Double.toString(p.getLatitude())));
        }
        processOrdinateParameters();
    }

    /**
     * Map this shape's values to ORACLE ORDINATE function parameters.  Oracle polygons are constructed in
     * counter-clockwise direction, so reverse the vertices here.
     *
     * @param parameterList The ExpressionList to add parameters to.
     */
    @Override
    void mapValues(final ExpressionList parameterList) {
        final List<Expression[]> vertexExpressionPoints = new ArrayList<>();

        // Start at 1 since the first item will be the coordinate system.
        for (int i = 1; i < this.vertices.size(); i = i + 2) {
            final Expression ra = this.vertices.get(i);
            final Expression dec = this.vertices.get(i + 1);
            vertexExpressionPoints.add(new Expression[] {
               ra, dec
            });
        }

        // Reverse to go in counter-clockwise direction as per Oracle specifications.
        Collections.reverse(vertexExpressionPoints);

        // Add the vertices after reversing them.
        for (final Expression[] vertexExpressionPoint : vertexExpressionPoints) {
            addNumericExpression(vertexExpressionPoint[0], parameterList);
            addNumericExpression(vertexExpressionPoint[1], parameterList);
        }
    }

    @SuppressWarnings("unchecked")
    void addNumericExpression(final Expression expression, final ExpressionList parameterList) {
        if (!(expression instanceof DoubleValue) && !(expression instanceof LongValue)) {
            throw new UnsupportedOperationException(
                    String.format("Cannot use non-constant coordinates in Polygon.  Expected Double or Long but found"
                                  + " '%s'", expression.toString()));
        } else {
            parameterList.getExpressions().add(expression);
        }
    }

    /**
     * Determine whether this shape matches the types provided by the structTypeArray.  The individual types should
     * have an array of numbers to compare.
     *
     * @param structTypeArray The numerical array from the database to check for.
     * @return True if the numbers match, False otherwise.
     */
    public static boolean structMatches(final BigDecimal[] structTypeArray) {
        final List<Integer> currValues = Arrays.stream(ORACLE_ELEMENT_INFO_VALUES).boxed().collect(Collectors.toList());
        final List<Integer> structValues = Arrays.stream(structTypeArray).map(BigDecimal::intValue).collect(
                Collectors.toList());
        return structValues.containsAll(currValues);
    }
}
