
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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import org.junit.Test;

import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;

import java.util.ArrayList;
import java.util.List;


public class OraclePolygonTest extends AbstractFunctionTest {
    @Test
    @SuppressWarnings("unchecked")
    public void convertParameters() {
        final List<Expression> expressionList = new ArrayList<>();

        expressionList.add(new StringValue("\"ICRS\""));
        expressionList.add(new DoubleValue("88.0"));
        expressionList.add(new DoubleValue("188.0"));
        expressionList.add(new DoubleValue("288.0"));
        expressionList.add(new DoubleValue("388.0"));
        expressionList.add(new DoubleValue("288.0"));
        expressionList.add(new DoubleValue("28.0"));

        final OraclePolygon testSubject = new OraclePolygon(expressionList);
        final List<Expression> expectedExpressions = new ArrayList<>();
        final List<Expression> resultExpressions = testSubject.getParameters().getExpressions();

        final Function ordinateArrayFunction = new Function();
        final ExpressionList ordinateArrayFunctionParams = new ExpressionList(new ArrayList<>());
        ordinateArrayFunction.setName(OraclePolygon.ORDINATE_ARRAY_FUNCTION_NAME);
        ordinateArrayFunctionParams.getExpressions().add(new DoubleValue("288.0"));
        ordinateArrayFunctionParams.getExpressions().add(new DoubleValue("28.0"));
        ordinateArrayFunctionParams.getExpressions().add(new DoubleValue("288.0"));
        ordinateArrayFunctionParams.getExpressions().add(new DoubleValue("388.0"));
        ordinateArrayFunctionParams.getExpressions().add(new DoubleValue("88.0"));
        ordinateArrayFunctionParams.getExpressions().add(new DoubleValue("188.0"));
        ordinateArrayFunction.setParameters(ordinateArrayFunctionParams);

        expectedExpressions.add(new LongValue("2003"));
        expectedExpressions.add(new LongValue(Long.toString(OracleGeometricFunction.SRID_VALUE)));
        expectedExpressions.add(new NullValue());
        expectedExpressions.add(getElemInfoFunction("1"));
        expectedExpressions.add(ordinateArrayFunction);

        assertResultExpressions(expectedExpressions, resultExpressions);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void convertParametersFromPolygon() {
        final Polygon polygon = new Polygon();
        polygon.getVertices().add(new Point(44.0D, 89.8D));
        polygon.getVertices().add(new Point(10.6D, 77.9D));
        polygon.getVertices().add(new Point(20.0D, -0.8D));

        final OraclePolygon testSubject = new OraclePolygon(polygon);
        final List<Expression> expectedExpressions = new ArrayList<>();
        final List<Expression> resultExpressions = testSubject.getParameters().getExpressions();

        final Function ordinateArrayFunction = new Function();
        final ExpressionList ordinageArrayFunctionParams = new ExpressionList(new ArrayList<>());
        final List<Expression> expectedVertices = new ArrayList<>();

        expectedVertices.add(new DoubleValue("20.0"));
        expectedVertices.add(new DoubleValue("-0.8"));
        expectedVertices.add(new DoubleValue("10.6"));
        expectedVertices.add(new DoubleValue("77.9"));
        expectedVertices.add(new DoubleValue("44.0"));
        expectedVertices.add(new DoubleValue("89.8"));

        ordinateArrayFunction.setName(OraclePolygon.ORDINATE_ARRAY_FUNCTION_NAME);
        ordinageArrayFunctionParams.getExpressions().addAll(expectedVertices);
        ordinateArrayFunction.setParameters(ordinageArrayFunctionParams);

        expectedExpressions.add(new LongValue("2003"));
        expectedExpressions.add(new LongValue(Long.toString(OracleGeometricFunction.SRID_VALUE)));
        expectedExpressions.add(new NullValue());
        expectedExpressions.add(getElemInfoFunction("1"));
        expectedExpressions.add(ordinateArrayFunction);

        assertResultExpressions(expectedExpressions, resultExpressions);
    }
}
