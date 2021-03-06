
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

package ca.nrc.cadc.tap.parser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;

import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Top;
import org.junit.Test;
import org.junit.Assert;

import ca.nrc.cadc.tap.expression.OracleExpressionDeParser;

import java.util.ArrayList;
import java.util.List;


public class OracleQuerySelectDeParserTest {

    @Test
    public void visitPlainSelect() {
        final StringBuffer buffer = new StringBuffer();
        final ExpressionVisitor expressionVisitor = new OracleExpressionDeParser(null, buffer);
        final OracleQuerySelectDeParser testSubject = new OracleQuerySelectDeParser(expressionVisitor, buffer);
        final PlainSelect plainSelect = new PlainSelect();
        final Table table = new Table(null, "t");
        final Top top = new Top();
        top.setRowCount(88);

        final List<SelectItem> selectItemList = new ArrayList<>();

        final SelectExpressionItem itemX = new SelectExpressionItem();
        itemX.setExpression(new Column(table, "x"));
        selectItemList.add(itemX);

        final SelectExpressionItem itemY = new SelectExpressionItem();
        itemY.setExpression(new Column(table, "y"));
        selectItemList.add(itemY);

        final EqualsTo whereClause = new EqualsTo();
        final Function left = new Function();
        left.setName("f");
        final LongValue right = new LongValue("5");
        whereClause.setLeftExpression(left);
        whereClause.setRightExpression(right);

        plainSelect.setSelectItems(selectItemList);
        plainSelect.setFromItem(table);
        plainSelect.setTop(top);
        plainSelect.setWhere(whereClause);
        testSubject.visit(plainSelect);

        Assert.assertEquals("Wrong query output",
                            "SELECT * FROM (SELECT t.x, t.y FROM t WHERE f() = 5) WHERE ROWNUM <= 88",
                            buffer.toString());
    }

    @Test
    public void visitTopWithFunction() {
        final StringBuffer buffer = new StringBuffer();
        final ExpressionVisitor expressionVisitor = new OracleExpressionDeParser(null, buffer);
        final OracleQuerySelectDeParser testSubject = new OracleQuerySelectDeParser(expressionVisitor, buffer);
        final PlainSelect plainSelect = new PlainSelect();
        final Table table = new Table(null, "t");
        final Top top = new Top();
        top.setRowCount(1000);

        final List<SelectItem> selectItemList = new ArrayList<>();

        final SelectExpressionItem itemY = new SelectExpressionItem();
        final Function abs = new Function();
        abs.setName("abs");
        final List<Expression> absArgumentList = new ArrayList<>();
        absArgumentList.add(new Column(table, "y"));
        final ExpressionList absArguments = new ExpressionList(absArgumentList);
        abs.setParameters(absArguments);
        itemY.setExpression(abs);

        selectItemList.add(itemY);

        final NotEqualsTo whereClause = new NotEqualsTo();
        whereClause.setLeftExpression(new Column(table, "y"));
        whereClause.setRightExpression(new NullValue());

        plainSelect.setSelectItems(selectItemList);
        plainSelect.setFromItem(table);
        plainSelect.setTop(top);
        plainSelect.setWhere(whereClause);
        testSubject.visit(plainSelect);

        Assert.assertEquals("Wrong query output",
                            "SELECT * FROM (SELECT abs(t.y) FROM t WHERE t.y <> NULL) WHERE ROWNUM <= 1000",
                            buffer.toString());
    }
}
