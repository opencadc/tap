
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
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import ca.nrc.cadc.dali.Point;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Abstract class for an Oracle Geometric Function using the SDO_GEOMETRY function.  This class supports a POINT or
 * arbitrary POLYGON (i.e. CIRCLE, BOX, or POLYGON).
 */
abstract class OracleGeometricFunction extends Function {
    private static final String ORACLE_GEOMETRY_FUNCTION_NAME = "SDO_GEOMETRY";
    static final String ORDINATE_ARRAY_FUNCTION_NAME = "SDO_ORDINATE_ARRAY";
    static final String ELEM_INFO_FUNCTION_NAME = "SDO_ELEM_INFO_ARRAY";
    static final String POINT_FUNCTION_NAME = "SDO_POINT_TYPE";
    static final long POINT_GEO_TYPE = 2001;
    static final long POLYGON_GEO_TYPE = 2003;

    private final List<Expression> elementInfoValues = new ArrayList<>();


    private OracleGeometricFunction(final List<Expression> elementInfoValues) {
        this.elementInfoValues.addAll(elementInfoValues);

        setName(ORACLE_GEOMETRY_FUNCTION_NAME);
        setParameters(new ExpressionList(new ArrayList()));
    }

    @SuppressWarnings("unchecked")
    OracleGeometricFunction(final Point point) {
        this(Collections.EMPTY_LIST);

        addParameter(new LongValue(Long.toString(POINT_GEO_TYPE)));
        addParameter(new NullValue());

        final Function pointFunction = new Function();
        pointFunction.setName(POINT_FUNCTION_NAME);

        final ExpressionList parameters = new ExpressionList(new ArrayList());
        parameters.getExpressions().add(new DoubleValue(Double.toString(point.getLongitude())));
        parameters.getExpressions().add(new DoubleValue(Double.toString(point.getLatitude())));
        parameters.getExpressions().add(new NullValue());

        pointFunction.setParameters(parameters);

        addParameter(pointFunction);
        addParameter(new NullValue());
        addParameter(new NullValue());
    }

    OracleGeometricFunction(final Expression[] elementInfoValues) {
        this(Arrays.asList(elementInfoValues));
        addParameter(new LongValue(Long.toString(POLYGON_GEO_TYPE)));
        addParameter(new NullValue());
        addParameter(new NullValue());
        addElemInfoFunction();
    }

    @SuppressWarnings("unchecked")
    private void addElemInfoFunction() {
        if (elementInfoValues.isEmpty()) {
            addParameter(new NullValue());
        } else {
            final Function elemInfoFunction = new Function();
            final ExpressionList elemInfoFunctionParameters = new ExpressionList(new ArrayList());
            elemInfoFunctionParameters.getExpressions().addAll(elementInfoValues);
            elemInfoFunction.setName(ELEM_INFO_FUNCTION_NAME);
            elemInfoFunction.setParameters(elemInfoFunctionParameters);

            addParameter(elemInfoFunction);
        }
    }

    /**
     * For Polygon shapes (i.e. non-point shapes), convert the values to be used as function parameters.  Point
     * Functions can omit this call.
     */
    void processOrdinateParameters() {
        final Function ordinateArrayFunction = new Function();
        ordinateArrayFunction.setName(ORDINATE_ARRAY_FUNCTION_NAME);
        final ExpressionList ordinateArrayFunctionParameters = new ExpressionList(new ArrayList());
        mapValues(ordinateArrayFunctionParameters);
        ordinateArrayFunction.setParameters(ordinateArrayFunctionParameters);
        addParameter(ordinateArrayFunction);
    }

    @SuppressWarnings("unchecked")
    void addParameter(final Expression expression) {
        getParameters().getExpressions().add(expression);
    }

    /**
     * Map this shape's values to ORACLE ORDINATE function parameters.
     *
     * @param parameterList The ExpressionList to add parameters to.
     */
    abstract void mapValues(final ExpressionList parameterList);
}
