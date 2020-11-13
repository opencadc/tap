
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

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import ca.nrc.cadc.dali.Circle;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class OracleCircle extends OracleGeometricFunction {

    private static final int[] ORACLE_ELEMENT_INFO_VALUES = new int[] {
            1, 1003, 4
    };

    private static final Expression[] ORACLE_ELEMENT_INFO = new Expression[ORACLE_ELEMENT_INFO_VALUES.length];

    static {
        for (int i = 0; i < ORACLE_ELEMENT_INFO_VALUES.length; i++) {
            ORACLE_ELEMENT_INFO[i] = new LongValue(Long.toString(ORACLE_ELEMENT_INFO_VALUES[i]));
        }
    }

    private final Expression ra;
    private final Expression dec;
    private final Expression radius;


    public OracleCircle(final Expression ra, final Expression dec, final Expression radius) {
        super(ORACLE_ELEMENT_INFO);
        this.ra = ra;
        this.dec = dec;
        this.radius = radius;

        processOrdinateParameters();
    }

    public OracleCircle(final Circle circle) {
        this(new DoubleValue(Double.toString(circle.getCenter().getLongitude())),
             new DoubleValue(Double.toString(circle.getCenter().getLatitude())),
             new DoubleValue(Double.toString(circle.getRadius())));
    }

    /**
     * Map this shape's values to ORACLE ORDINATE function parameters.  For Oracle CIRCLEs, we need to calculate three
     * points along the circle's circumference.
     *
     * @param parameterList The ExpressionList to add parameters to.
     */
    @Override
    @SuppressWarnings("unchecked")
    void mapValues(final ExpressionList parameterList) {
        // Oracle GEO CIRCLE
        final List<Expression> expressions = parameterList.getExpressions();

        // First point (x - radius, y)
        expressions.add(calculateXPointOne());
        expressions.add(dec);

        // Second point (x, y + radius)
        expressions.add(ra);
        expressions.add(calculateYPointOne());

        // Third point (x + radius, y)
        expressions.add(calculateXPointTwo());
        expressions.add(dec);
    }

    Expression calculateXPointOne() {
        return new DoubleValue(Double.toString(parseRA() - parseRadius()));
    }

    Expression calculateYPointOne() {
        return new DoubleValue(Double.toString(parseDec() + parseRadius()));
    }

    Expression calculateXPointTwo() {
        return new DoubleValue(Double.toString(parseRA() + parseRadius()));
    }

    double parseRadius() {
        return Double.parseDouble(radius.toString());
    }

    double parseRA() {
        return Double.parseDouble(ra.toString());
    }

    double parseDec() {
        return Double.parseDouble(dec.toString());
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
