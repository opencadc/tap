/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2024.                            (c) 2024.
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

package ca.nrc.cadc.tap.parser.function;

import ca.nrc.cadc.tap.parser.OperatorVisitor;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import org.apache.log4j.Logger;

/**
 * Expression for arithmetic, conditional, and relational operators.
 * Operator will create a binary expression with the two expressions
 * separated by the operator, or the negation operator if the negate
 * method is invoked.
 * 
 * @author jburke
 */
public class Operator extends BinaryExpression
{
    private static Logger log = Logger.getLogger(Operator.class);

    private boolean negate;
    protected String operator;
    protected String negateOperator;

    /**
     * Constructor.
     * 
     * @param operator The operator between expressions.
     * @param negateOperator The negate operator between expressions.
     * @param left The left expression.
     * @param right The right expression.
     */
    public Operator(String operator, String negateOperator, Expression left, Expression right)
    {
        super();
        this.operator = operator;
        this.negateOperator = negateOperator;
        if (left instanceof OperatorArg)
            ((OperatorArg) left).setOperand(true);
        setLeftExpression(left);
        if (right instanceof OperatorArg)
            ((OperatorArg) right).setOperand(true);
        setRightExpression(right);
        negate = false;
    }

    /**
     * @return If the expression has been negated return the negate operator,
     *         else return the operator.
     */
    public String getOperator()
    {
        if (negate)
            return negateOperator;
        return operator;
    }

    /**
     * Invoke this method to use the negation operator in the expression.
     */
    public void negate()
    {
        negate = true;
    }

    /**
     * @return The operator.
     */
    @Override
    public String getStringExpression()
    {
        return getOperator();
    }

    /**
     * Operator can only accept a QueryParser or one of its sub-classes.
     *
     * @param expressionVisitor
     */
    public void accept(ExpressionVisitor expressionVisitor)
    {
        log.debug("accept(" + expressionVisitor.getClass().getSimpleName() + "): " + this);
        if (expressionVisitor instanceof OperatorVisitor) // visitor pattern extension
            ((OperatorVisitor) expressionVisitor).visit(this);
        else
        {
            getLeftExpression().accept(expressionVisitor);
            getRightExpression().accept(expressionVisitor);
        }
    }
    
}
