/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ca.nrc.cadc.tap.parser;

import java.util.Iterator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Top;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.apache.log4j.Logger;


/**
 * The methods in this class override JSQLParser SelectDeParser methods to
 * fix de-parsing bugs.
 *
 * @author jburke
 */
public class QuerySelectDeParser extends SelectDeParser
{
    private static Logger log = Logger.getLogger(QuerySelectDeParser.class);

    private boolean tableAliasWithAS = true;

    public QuerySelectDeParser()
    {
        super();
    }

    public QuerySelectDeParser(ExpressionVisitor expressionVisitor,
                               StringBuilder buffer)
    {
        super(expressionVisitor, buffer);
    }

    public void setTableAliasWithAS(boolean enabled)
    {
        this.tableAliasWithAS = enabled;
    }

    /**
     * The table alias, if it exists, was not appended to the table name.
     *
     * @param table
     */
    @Override
    public void visit(Table table)
    {
        log.debug("visit(Table) " + table);
        buffer(table.getFullyQualifiedName());
        if (table.getAlias() != null)
        {
            if (tableAliasWithAS)
            {
                buffer(" AS ");
            }
            else
            {
                buffer(" ");
            }
            buffer(table.getAlias().getName());
        }
    }

    /**
     * Incorrectly appends the From alias after the From has been appended to the query,
     * instead of allowing the From expression visitor to process the alias, i.e. Table.
     *
     * @param join
     */
    @Override
    public void deparseJoin(Join join)
    {
        if (join.isSimple())
        {
            buffer(", ");
        }
        else
        {
            if (join.isRight())
            {
                buffer(" RIGHT");
            }
            else if (join.isNatural())
            {
                buffer(" NATURAL");
            }
            else if (join.isFull())
            {
                buffer(" FULL");
            }
            else if (join.isLeft())
            {
                buffer(" LEFT");
            }

            if (join.isOuter())
            {
                buffer(" OUTER");
            }
            else if (join.isInner())
            {
                buffer(" INNER");
            }

            buffer(" JOIN ");
        }

        FromItem fromItem = join.getRightItem();
        fromItem.accept(this);
        if (join.getOnExpression() != null)
        {
            buffer(" ON ");
            join.getOnExpression().accept(getExpressionVisitor());
        }
        if (join.getUsingColumns() != null)
        {
            buffer(" USING ( ");
            for (Iterator iterator = join.getUsingColumns().iterator(); iterator
                    .hasNext(); )
            {
                Column column = (Column) iterator.next();
                buffer(column.getFullyQualifiedName());
                if (iterator.hasNext())
                {
                    buffer(" ,");
                }
            }
            buffer(")");
        }
    }

    /**
     * TOP, if it exists, was not inserted into the query.
     *
     * @param plainSelect
     */
    @Override
    public void visit(PlainSelect plainSelect)
    {
        log.debug("visit(" + plainSelect.getClass()
                .getSimpleName() + ") " + plainSelect);
        buffer("SELECT ");
        Top top = plainSelect.getTop();
        if (top != null)
        {
            buffer("TOP ");
            buffer(Long.toString(top.getRowCount()));
            buffer(" ");
        }
        if (plainSelect.getDistinct() != null)
        {
            buffer("DISTINCT ");
            if (plainSelect.getDistinct().getOnSelectItems() != null)
            {
                buffer("ON (");
                for (Iterator iter = plainSelect.getDistinct()
                        .getOnSelectItems().iterator(); iter.hasNext(); )
                {
                    SelectItem selectItem = (SelectItem) iter.next();
                    selectItem.accept(this);
                    if (iter.hasNext())
                    {
                        buffer(", ");
                    }
                }
                buffer(") ");
            }
        }

        for (Iterator iter = plainSelect.getSelectItems().iterator(); iter
                .hasNext(); )
        {
            SelectItem selectItem = (SelectItem) iter.next();
            selectItem.accept(this);
            if (iter.hasNext())
            {
                buffer(", ");
            }
        }

        buffer(" ");

        if (plainSelect.getFromItem() != null)
        {
            buffer("FROM ");
            plainSelect.getFromItem().accept(this);
        }

        if (plainSelect.getJoins() != null)
        {
            for (final Join join : plainSelect.getJoins())
            {
                deparseJoin(join);
            }
        }

        if (plainSelect.getWhere() != null)
        {
            buffer(" WHERE ");
            plainSelect.getWhere().accept(getExpressionVisitor());
        }

        if (plainSelect.getGroupByColumnReferences() != null)
        {
            buffer(" GROUP BY ");
            for (Iterator<Expression> iter
                 = plainSelect.getGroupByColumnReferences().iterator();
                 iter.hasNext(); )
            {
                Expression columnReference =  iter.next();
                columnReference.accept(getExpressionVisitor());
                if (iter.hasNext())
                {
                    buffer(", ");
                }
            }
        }

        if (plainSelect.getHaving() != null)
        {
            buffer(" HAVING ");
            plainSelect.getHaving().accept(getExpressionVisitor());
        }
    }

    void buffer(final String s)
    {
        getBuffer().append(s);
    }
}
