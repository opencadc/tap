/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.tap.parser;

import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class FunctionFinder extends SelectNavigator 
{
    private static final Logger log = Logger.getLogger(FunctionFinder.class);
    
    public FunctionFinder(ExpressionNavigator en, ReferenceNavigator rn, FromItemNavigator fn)
    {
        super(en, rn, fn);
    }

    
    /**
     * Overwrite method in super class SelectNavigator.  
     * It navigates all parts of the select statement,
     * trying to locate all occurrence of region functions.
     * 
     * @param plainSelect
     */
    @SuppressWarnings("unchecked")
    @Override
    public void visit(PlainSelect plainSelect)
    {
        log.debug("visit(PlainSelect): " + plainSelect);
        super.enterPlainSelect(plainSelect);

        // Visiting select items
        this.visitingPart = VisitingPart.SELECT_ITEM;
        ListIterator i = plainSelect.getSelectItems().listIterator();
        while (i.hasNext())
        {
            Object obj = i.next();
            if (obj instanceof SelectExpressionItem)
            {
                SelectExpressionItem s = (SelectExpressionItem) obj;
                Expression ex = s.getExpression();
                Expression implExpression = convertToImplementation(ex);
                s.setExpression(implExpression);
            }
        }

        this.visitingPart = VisitingPart.FROM;
        List<Join> joins = plainSelect.getJoins();
        if (joins != null)
        {
            for (Join join : joins)
            {
                Expression e = join.getOnExpression();
                Expression implExpression = convertToImplementation(e);
                log.debug("PlainSelect/JOIN: replacing " + e + " with " + implExpression);
                join.setOnExpression(implExpression);
            }
        }

        this.visitingPart = VisitingPart.WHERE;
        if (plainSelect.getWhere() != null)
        {
            Expression e = plainSelect.getWhere();
            Expression implExpression = convertToImplementation(e);
            log.debug("PlainSelect/WHERE: replacing " + e + " with " + implExpression);
            plainSelect.setWhere(implExpression);
        }

        this.visitingPart = VisitingPart.HAVING;
        if (plainSelect.getHaving() != null)
        {
            Expression e = plainSelect.getHaving();
            Expression implExpression = convertToImplementation(e);
            log.debug("PlainSelect/HAVING: replacing " + e + " with " + implExpression);
            plainSelect.setHaving(implExpression);
        }

        log.debug("visit(PlainSelect) done: " + plainSelect);
        super.leavePlainSelect();
    }

    /**
     * Convert an expression and all parameters of it, 
     * using provided implementation in the sub-class.
     * 
     * @param expr
     * @return Expression converted by implementation 
     */
    public Expression convertToImplementation(Expression expr)
    {
        log.debug("convertToImplementation(Expression):" + expr);

        Expression implExpr = expr;

        if (expr instanceof Function)
        {
            Function f = (Function) expr;

            // Convert parameters of the function first.
            ExpressionList exprList = f.getParameters();
            ExpressionList implExprList = convertToImplementation(exprList);
            f.setParameters(implExprList);
            implExpr = convertToImplementation(f);
        }
        else if (expr instanceof BinaryExpression)
        {
            BinaryExpression expr1 = (BinaryExpression) expr;

            Expression left = expr1.getLeftExpression();
            Expression right = expr1.getRightExpression();

            Expression left2 = convertToImplementation(left);
            Expression right2 = convertToImplementation(right);

            expr1.setLeftExpression(left2);
            expr1.setRightExpression(right2);
            implExpr = expr1;

            implExpr = handlePredicateFunction((BinaryExpression) implExpr);
        }
        else if (expr instanceof InverseExpression)
        {
            InverseExpression expr1 = (InverseExpression) expr;
            Expression child = expr1.getExpression();
            Expression child2 = convertToImplementation(child);
            expr1.setExpression(child2);
            implExpr = expr1;
        }
        else if (expr instanceof Parenthesis)
        {
            Parenthesis expr1 = (Parenthesis) expr;
            Expression child = expr1.getExpression();
            Expression child2 = convertToImplementation(child);
            expr1.setExpression(child2);
            implExpr = expr1;
        }
        return implExpr;
    }

    /**
     * Convert a list of expressions and all parameters of them 
     * using provided implementation in the sub-class.
     * 
     * @param exprList
     * @return converted expression list
     */
    @SuppressWarnings("unchecked")
    public ExpressionList convertToImplementation(ExpressionList exprList)
    {
        log.debug("convertToImplementation(ExpressionList): " + exprList);
        if (exprList == null || exprList.getExpressions() == null) return exprList;
        List<Expression> adqlExprs = exprList.getExpressions();
        List<Expression> implExprs = new ArrayList<Expression>();
        Expression e1 = null;
        Expression e2 = null;

        for (int i = 0; i < adqlExprs.size(); i++)
        {
            e1 = adqlExprs.get(i);
            e2 = convertToImplementation(e1);
            implExprs.add(e2);
        }
        return new ExpressionList(implExprs);
    }

    /**
     * Convert an predicate involving a function.
     *
     * @param expr
     * @return Expression converted by implementation
     */
    protected Expression handlePredicateFunction(BinaryExpression expr)
    {
        return expr;
    }

    /**
     * Convert a non-predicate function.
     *
     * @param func
     * @return converted expression
     */
    protected Expression convertToImplementation(Function func)
    {
        return func;
    }

}
