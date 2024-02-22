/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.tap.parser.extractor;

import ca.nrc.cadc.tap.TapSelectItem;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.schema.TapSchemaUtil;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.FunctionDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.log4j.Logger;

/**
 * Extract a list of TapSelectItem from query.
 * 
 * @author zhangsa
 *
 */
public class SelectListExpressionExtractor extends ExpressionNavigator
{
    private static final Logger log = Logger.getLogger(SelectListExpressionExtractor.class);
    
    protected TapSchema tapSchema;
    protected List<TapSelectItem> selectList;
    protected int columnIndex;
    protected String alias;

    /**
     * @param tapSchema
     */
    public SelectListExpressionExtractor(TapSchema tapSchema)
    {
	this(tapSchema, 1);
    }

    /**
     * @param tapSchema
     * @param columnIndex
     */
    public SelectListExpressionExtractor(TapSchema tapSchema, int columnIndex)
    {
        super();
        this.tapSchema = tapSchema;
        this.selectList = new ArrayList<TapSelectItem>();
        this.columnIndex = columnIndex;
        this.alias = null;
    }

    /* (non-Javadoc)
     * @see net.sf.jsqlparser.statement.select.SelectItemVisitor#visit(net.sf.jsqlparser.statement.select.AllColumns)
     */
    @Override
    public void visit(AllColumns allColumns)
    {
        throw new UnsupportedOperationException("AllColumns must have previously been visited");
    }

    /* (non-Javadoc)
     * @see net.sf.jsqlparser.statement.select.SelectItemVisitor#visit(net.sf.jsqlparser.statement.select.AllTableColumns)
     */
    @Override
    public void visit(AllTableColumns allTableColumns)
    {
        throw new UnsupportedOperationException("AllTableColumns must have previously been visited");
    }

    /* (non-Javadoc)
     * @see net.sf.jsqlparser.statement.select.SelectItemVisitor#visit(net.sf.jsqlparser.statement.select.SelectExpressionItem)
     */
    @Override
    public void visit(SelectExpressionItem selectExpressionItem)
    {
        log.debug("visit(selectExpressionItem)" + selectExpressionItem);

        TapSelectItem paramDesc = null;
        PlainSelect plainSelect = selectNavigator.getPlainSelect();

        Expression expression = selectExpressionItem.getExpression();
        if (expression instanceof SubSelect)
        {
            SubSelect subSelect = (SubSelect) expression;
            log.debug("visit(subSelect) " + subSelect);

            SelectListExtractor sle = new SelectListExtractor(new SelectListExpressionExtractor(tapSchema, getColumnIndex()),
                                                              new ReferenceNavigator(),
                                                              new FromItemNavigator());
            subSelect.getSelectBody().accept(sle);
            SelectListExpressionExtractor slee = (SelectListExpressionExtractor) sle.getExpressionNavigator();
            List <TapSelectItem> tmpList = slee.getSelectList();
            if (tmpList.size() != 1)
            {
                final String error = "Expected 1 ParamDesc in SelectList, found " + tmpList.size();
                throw new IllegalStateException(error);
            }
            paramDesc = tmpList.get(0);
        }
        else
        {
            setAlias(selectExpressionItem.getAlias());
            paramDesc = getItemFromExpression(expression, plainSelect);
        }

        log.debug("select item: " + paramDesc.getColumnName() + " " + paramDesc.getDatatype());
        selectList.add(paramDesc);
        setColumnIndex(selectList.size() + 1);
    }

    public List<TapSelectItem> getSelectList()
    {
        return selectList;
    }

    public void setSelectList(List<TapSelectItem> selectList)
    {
        this.selectList = selectList;
    }

    public TapSchema getTapSchema()
    {
        return this.tapSchema;
    }

    public void setTapSchema(TapSchema tapSchema)
    {
        this.tapSchema = tapSchema;
    }

    public int getColumnIndex()
    {
        return this.columnIndex;
    }

    public void setColumnIndex(int columnIndex)
    {
        this.columnIndex = columnIndex;
    }

    public String getAlias()
    {
        return this.alias;
    }

    public void setAlias(String alias)
    {
        this.alias = alias;
    }

    public String getColumnName(String prefix)
    {
	String alias = getAlias();

        if(alias == null || alias.isEmpty())
            return prefix + getColumnIndex();
        else
            return alias;
    }

    public String getGenericColumnName()
    {
        return getColumnName("col");
    }

    public boolean isIntegerNumericType(TapDataType dt)
    {
        return (TapDataType.BOOLEAN.equals(dt) ||
                TapDataType.INTEGER.equals(dt) ||
                TapDataType.SHORT.equals(dt) ||
                TapDataType.LONG.equals(dt));
    }

    public boolean isNonIntegerNumericType(TapDataType dt)
    {
        return (TapDataType.DOUBLE.equals(dt) || TapDataType.FLOAT.equals(dt));
    }

    private TapSelectItem getItemFromFunction(Function function, PlainSelect plainSelect)
    {
        FunctionDesc functionDesc = TapSchemaUtil.findFunctionDesc(tapSchema, function);
        String name = getColumnName(function.getName());

        log.debug("getItemFromFunction: " + function.getName() + " -> " + functionDesc);
        if (functionDesc == null)
            throw new UnsupportedOperationException("invalid function: " + function.getName());

        if ( TapDataType.FUNCTION_ARG.equals(functionDesc.getDatatype()) )
        {
            // Some functions return the type of their arguments rather than a
            // static type.
            for (Object parameter : function.getParameters().getExpressions())
            {
                TapSelectItem item = getItemFromExpression((Expression) parameter, plainSelect);
                return new TapSelectItem(name, item.getDatatype());
            }
        }

        return new TapSelectItem(name, functionDesc.getDatatype());
    }

    private TapSelectItem getItemFromExpression(Expression expression, PlainSelect ps)
    {
        if (expression instanceof Column)
        {
            Column column = (Column)expression;
            String name = column.getColumnName();
            if (getAlias() != null && !getAlias().isEmpty())
                name = getAlias();

            ColumnDesc columnDesc = TapSchemaUtil.findColumnDesc(tapSchema, ps, column);
            return new TapSelectItem(name, columnDesc);
        }
        else if (expression instanceof Function)
        {
            return getItemFromFunction((Function)expression, ps);
        }
        else if (expression instanceof Parenthesis)
        {
            Parenthesis parenthesis = (Parenthesis)expression;
            return getItemFromExpression(parenthesis.getExpression(), ps);
        }
        else if (expression instanceof CaseExpression)
        {
            CaseExpression ce = (CaseExpression)expression;
            List<WhenClause> clauses = ce.getWhenClauses();
            List<Expression> expressions = new ArrayList<Expression>();

            for (WhenClause wc : clauses)
                expressions.add(wc.getThenExpression());

            if (ce.getElseExpression() != null)
                expressions.add(ce.getElseExpression());

            TapDataType datatype = TapDataType.LONG;

            for (Expression exp : expressions)
            {
                TapSelectItem i = getItemFromExpression(exp, ps);
                TapDataType dt = i.getDatatype();

                if (!isIntegerNumericType(i.getDatatype()) &&
                    !isNonIntegerNumericType(i.getDatatype()))
                {
                    // If any of the then expressions return a non-numeric, we
                    // have to assume the worst and always return a string.
                    return new TapSelectItem(getGenericColumnName(), TapDataType.STRING);
                }
                else if(isNonIntegerNumericType(i.getDatatype()))
                {
                    // If any of the then expressions return a double,
                    // the datatype is double
                    datatype = TapDataType.DOUBLE;
                }
            }

            return new TapSelectItem(getGenericColumnName(), datatype);
        }
        else if (expression instanceof DoubleValue)
        {
            return new TapSelectItem(getGenericColumnName(), TapDataType.DOUBLE);
        }
        else if (expression instanceof LongValue)
        {
            return new TapSelectItem(getGenericColumnName(), TapDataType.LONG);
        }
        else if (expression instanceof Addition ||
                 expression instanceof Subtraction ||
                 expression instanceof Multiplication)
        {
            BinaryExpression be = (BinaryExpression)expression;
            TapSelectItem left = getItemFromExpression(be.getLeftExpression(), ps);
            TapSelectItem right = getItemFromExpression(be.getRightExpression(), ps);
            TapDataType leftType = left.getDatatype();
            TapDataType rightType = right.getDatatype();

            log.debug("leftType: " + leftType + " rightType: " + rightType);

            TapDataType datatype;

            if (isIntegerNumericType(leftType) && isIntegerNumericType(rightType))
                datatype = TapDataType.LONG;
            else
                datatype = TapDataType.DOUBLE;

            return new TapSelectItem(getGenericColumnName(), datatype);
        }
        else if (expression instanceof Division)
        {
            return new TapSelectItem(getGenericColumnName(), TapDataType.DOUBLE);
        }
        else
        {
            // Default to a string representation with a generic name
            // that can carry anything.
            return new TapSelectItem(getGenericColumnName(), TapDataType.STRING);
        }
    }
}
