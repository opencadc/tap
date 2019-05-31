
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.Point;


public class OracleCircleTest extends AbstractFunctionTest {
    @Test
    @SuppressWarnings("unchecked")
    public void convertParameters() {
        final OracleCircle testSubject = new OracleCircle(new DoubleValue("3.4"), new DoubleValue("88.5"),
                                                          new DoubleValue("0.7"));
        final ExpressionList result = testSubject.getParameters();
        final List<Expression> resultExpressions = result.getExpressions();
        final List<Expression> expectedExpressions = new ArrayList<>();

        final Function expectedOrdinateArrayFunction = new Function();
        final ExpressionList expectedOrdinateArrayFunctionParams = new ExpressionList(new ArrayList());
        expectedOrdinateArrayFunction.setName(OraclePolygon.ORDINATE_ARRAY_FUNCTION_NAME);
        expectedOrdinateArrayFunctionParams.getExpressions().addAll(Arrays.asList(new DoubleValue("" + (3.4D - 0.7D)),
                                                                                  new DoubleValue("88.5"),
                                                                                  new DoubleValue("3.4"),
                                                                                  new DoubleValue("" + (88.5D + 0.7D)),
                                                                                  new DoubleValue("" + (3.4D + 0.7D)),
                                                                                  new DoubleValue("88.5")));
        expectedOrdinateArrayFunction.setParameters(expectedOrdinateArrayFunctionParams);

        expectedExpressions.add(new LongValue("" + OracleGeometricFunction.POLYGON_GEO_TYPE));
        expectedExpressions.add(new NullValue());
        expectedExpressions.add(new NullValue());
        expectedExpressions.add(getElemInfoFunction("4"));
        expectedExpressions.add(expectedOrdinateArrayFunction);

        assertResultExpressions(expectedExpressions, resultExpressions);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void convertParametersFromCircle() {
        final OracleCircle testSubject = new OracleCircle(new Circle(new Point(65.78D, 34.56D),
                                                                     0.15D));
        final ExpressionList result = testSubject.getParameters();
        final List<Expression> resultExpressions = result.getExpressions();
        final List<Expression> expectedExpressions = new ArrayList<>();

        final Function ordinateArrayFunction = new Function();
        final ExpressionList ordinateArrayFunctionParams = new ExpressionList(new ArrayList());
        ordinateArrayFunction.setName(OraclePolygon.ORDINATE_ARRAY_FUNCTION_NAME);
        ordinateArrayFunctionParams.getExpressions().addAll(Arrays.asList(new DoubleValue("" + (65.78D - 0.15D)),
                                                                          new DoubleValue("34.56"),
                                                                          new DoubleValue("65.78"),
                                                                          new DoubleValue("" + (34.56D + 0.15D)),
                                                                          new DoubleValue("" + (65.78D + 0.15D)),
                                                                          new DoubleValue("34.56")));
        ordinateArrayFunction.setParameters(ordinateArrayFunctionParams);

        expectedExpressions.add(new LongValue("" + OracleGeometricFunction.POLYGON_GEO_TYPE));
        expectedExpressions.add(new NullValue());
        expectedExpressions.add(new NullValue());
        expectedExpressions.add(getElemInfoFunction("4"));
        expectedExpressions.add(ordinateArrayFunction);

        assertResultExpressions(expectedExpressions, resultExpressions);
    }
}
