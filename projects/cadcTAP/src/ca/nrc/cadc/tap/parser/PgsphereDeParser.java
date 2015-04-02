/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ca.nrc.cadc.tap.parser;

import ca.nrc.cadc.tap.parser.region.pgsphere.function.Spoint;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Spoly;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import org.apache.log4j.Logger;

/**
 * De-parser for PostgreSQL PGSphere functions.
 * 
 * @author jburke
 */
public class PgsphereDeParser extends BaseExpressionDeParser
{
    private static Logger log = Logger.getLogger(PgsphereDeParser.class);

    public PgsphereDeParser(SelectVisitor selectVisitor, StringBuffer buffer)
    {
        super(selectVisitor, buffer);
    }
    
    @Override
    public void visit(Column column)
    {
        log.debug("visit(" +  column.getClass().getSimpleName() + ") " + column);
        
        // postgresql: quoted identifiers avoid case foling so mixed-case columns
        // will not be found, so HACK: just strip the quotes because no sane person 
        // would actually rely on quoted identifers :-)
        // TODO: this should really go into a super class PostresqlQueryDeparser
        // but we don't have that right now... or be na option on BaseExpressionDeParser
        String cn = column.getColumnName();
        if (cn.indexOf('"') >= 0)
        {
            cn = cn.replace("\"", "");
            column.setColumnName(cn);
        }
        
        super.visit(column);
    }

    /**
     * De-parses PGSphere functions, else passes the function
     * to the super class for de-parsing.
     *
     * @param function
     */
    @SuppressWarnings("unchecked")
    @Override
    public void visit(Function function)
    {
        log.debug("visit(" + function.getClass().getSimpleName() + "): " + function);

        /**
         * De-parse a spoint, wrapping a cast around the spoint if
         * the spoint is used as an operand in another function.
         */
        if(function instanceof Spoint)
        {
            Spoint spoint = (Spoint) function;
            if (spoint.isOperand())
            {
                buffer.append("cast(");
                super.visit(spoint);
                buffer.append(" as scircle)");
            }
            else
            {
                super.visit(spoint);
            }
        }

        /**
         * De-parse a spoly.
         */
        else if(function instanceof Spoly)
        {
            Spoly spoly = (Spoly) function;
            buffer.append(spoly.getName());
            buffer.append(" '{");
            List<Expression> expressions = spoly.getParameters().getExpressions();
            String deli = "";
            for (Expression expression : expressions)
            {
                buffer.append(deli);
                deli = ",";
                if (expression instanceof StringValue)
                {
                    StringValue stringValue = (StringValue) expression;
                    stringValue.accept(this);
                }
                else if (expression instanceof Spoint)
                {
                    Spoint spoint = (Spoint) expression;
                    buffer.append(spoint.toVertex());
                }
            }
            buffer.append("}'");
        }
        else
        {
            super.visit(function);
        }
    }

}
