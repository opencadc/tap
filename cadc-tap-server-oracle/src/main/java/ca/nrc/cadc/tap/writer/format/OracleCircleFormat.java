
/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2018.                            (c) 2018.
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

package ca.nrc.cadc.tap.writer.format;

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.util.CircleFormat;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class OracleCircleFormat extends AbstractResultSetFormat {

    // Oracle's POINT type accepts a radius as an argument.
    static final String CIRCLE_FUNCTION_TYPE = "SDO_POINT_TYPE";
    static final String SHAPE_NAME = "Circle";

    private final CircleFormat circleFormat = new CircleFormat();


    @Override
    public Object extract(final ResultSet resultSet, final int columnIndex) throws SQLException {
        final String s = resultSet.getString(columnIndex);
        return getCircle(s);
    }

    @Override
    public String format(final Object object) {
        if (object instanceof Circle) {
            return OracleCircleFormat.SHAPE_NAME + " " + circleFormat.format((Circle) object);
        } else if (object instanceof BigDecimal[]) {
            return fromStruct((BigDecimal[]) object);
        } else {
            return object.toString();
        }
    }

    private String fromStruct(final BigDecimal[] structVerticeValues) {
        if (structVerticeValues.length != 6) {
            throw new IllegalArgumentException(
                    String.format("Should have six (6) values from the database, but has %d.",
                                  structVerticeValues.length));
        } else {
            // X - Radius
            final double pointOneX = structVerticeValues[0].doubleValue();

            // Y
            final double pointOneY = structVerticeValues[1].doubleValue();

            // X
            final double pointTwoX = structVerticeValues[2].doubleValue();

            final double radius = pointTwoX - pointOneX;

            return format(new Circle(new Point(pointTwoX, pointOneY), radius));
        }
    }

    Circle getCircle(final String clause) {
        final double[] coordinates = parseRadianValues(clause);
        return new Circle(new Point(coordinates[0], coordinates[1]), coordinates[2]);
    }

    /**
     * Parse the function arguments out and return them as radians.
     *
     * @param clause The String SQL clause.
     * @return double array of three items (x, y, and radius).
     */
    private double[] parseRadianValues(final String clause) {
        final Pattern p = Pattern.compile(OracleCircleFormat.CIRCLE_FUNCTION_TYPE, Pattern.CASE_INSENSITIVE);
        final Matcher m = p.matcher(clause);
        if (m.find()) {
            final int openFunctionIndex = m.start();
            final int openFunctionParenIndex = clause.indexOf("(", openFunctionIndex);
            final int closeFunctionParenIndex = clause.indexOf(")", openFunctionParenIndex);
            final String functionArgumentString =
                    clause.substring(openFunctionParenIndex + 1, closeFunctionParenIndex);
            final String[] functionArguments = functionArgumentString.split(",");

            if (functionArguments.length != 3) {
                throw new IllegalArgumentException(
                        String.format("Circles/Points should have three arguments in Oracle: '%s'", clause));
            } else {
                final double[] coords = new double[3];
                coords[0] = Math.toDegrees(Double.valueOf(functionArguments[0]));
                coords[1] = Math.toDegrees(Double.valueOf(functionArguments[1]));

                try {
                    coords[2] = Math.toDegrees(Double.parseDouble(functionArguments[2]));
                } catch (NumberFormatException e) {
                    // No parsable radius, so it's a point.
                }

                return coords;
            }
        } else {
            throw new IllegalArgumentException(
                    String.format("Missing %s function type for Circle clause '%s'",
                                  OracleCircleFormat.CIRCLE_FUNCTION_TYPE,
                                  clause));
        }
    }
}
