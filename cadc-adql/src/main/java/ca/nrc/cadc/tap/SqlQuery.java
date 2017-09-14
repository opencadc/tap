/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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

package ca.nrc.cadc.tap;

import ca.nrc.cadc.tap.parser.BaseExpressionDeParser;
import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.QuerySelectDeParser;
import ca.nrc.cadc.tap.parser.converter.AllColumnConverter;
import ca.nrc.cadc.tap.parser.converter.TableNameConverter;
import ca.nrc.cadc.tap.parser.extractor.SelectListExpressionExtractor;
import ca.nrc.cadc.tap.parser.extractor.SelectListExtractor;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import ca.nrc.cadc.tap.parser.schema.TapSchemaColumnValidator;
import ca.nrc.cadc.tap.parser.schema.TapSchemaTableValidator;
import ca.nrc.cadc.tap.schema.ParamDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.uws.ParameterUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.apache.log4j.Logger;

/**
 * TapQuery implementation for direct SQL query processing. The default implementation
 * validates the query against the TapSchema, converts wildcards in the select
 * list to a fixed list of columns, and extracts the select-list so it can be 
 * matched to TapSchema.columns to support output of the ResultSet.
 * 
 * @author pdowler
 *
 */
public class SqlQuery extends AbstractTapQuery
{
    protected static Logger log = Logger.getLogger(SqlQuery.class);

    protected String queryString;
    protected Statement statement;
    protected List<ParamDesc> selectList;
    protected List<SelectNavigator> navigatorList = new ArrayList<SelectNavigator>();

    protected transient boolean navigated = false;

    public SqlQuery() { }

    /**
     * Set up the List&#60;SelectNavigator&#62;. Subclasses should override this method to
     * add extra navigators that check or modify the parsed query statement. This
     * implementation creates: TapSchemaValidator, AllColumnConverter.
     */
    protected void init()
    {
        SelectNavigator sn = new SelectNavigator(new ExpressionNavigator(),
                                                 new TapSchemaColumnValidator(tapSchema),
                                                 new TapSchemaTableValidator(tapSchema));
        navigatorList.add(sn);

        // convert * to fixed select-list
        sn = new AllColumnConverter(new ExpressionNavigator(),
                                    new ReferenceNavigator(),
                                    new FromItemNavigator(),
                                    tapSchema);
        navigatorList.add(sn);

        // extract select-list
        sn = new SelectListExtractor(new SelectListExpressionExtractor(tapSchema),
                                     new ReferenceNavigator(),
                                     new FromItemNavigator());
        navigatorList.add(sn);
        
        // support for file uploads to map the upload table name to the query table name.
        if (extraTables != null && !extraTables.isEmpty())
        {
            TableNameConverter tnc = new TableNameConverter(true);
            Set<Map.Entry<String, TableDesc>> entries = extraTables.entrySet();
            for (Map.Entry entry : entries)
            {
                String newName = (String) entry.getKey();
                TableDesc tableDesc = (TableDesc) entry.getValue();
                tnc.put(tableDesc.getTableName(), newName);
                log.debug("TableNameConverter " + tableDesc.getTableName() + " -> " + newName);
            }
            sn = new SelectNavigator(new ExpressionNavigator(), new ReferenceNavigator(), tnc);
            navigatorList.add(sn);
        }
    }

    protected void doNavigate()
    {
        if (navigated) // idempotent
            return;

        init();

        // parse for syntax
        try
        {
            this.queryString = ParameterUtil.findParameterValue("QUERY", job.getParameterList());
            if (queryString == null || queryString.length() == 0) 
                throw new IllegalArgumentException("missing required parameter: QUERY");
            statement = ParserUtil.receiveQuery(queryString);
        }
        catch (JSQLParserException e)
        {
            throw new IllegalArgumentException("failed to parse SQL", e);
        }

        // run all the navigators
        for (SelectNavigator sn : navigatorList)
        {
            log.debug("Navigated by: " + sn.getClass().getName());

            ParserUtil.parseStatement(statement, sn);

            if (sn instanceof SelectListExtractor)
            {
                SelectListExpressionExtractor slen = (SelectListExpressionExtractor) sn.getExpressionNavigator();
                selectList = slen.getSelectList();
            }
        }
        navigated = true;
    }

    /**
     * Provide implementation of select deparser if the default (QuerySelectDeParser) is not sufficient.
     * 
     * @return 
     */
    protected QuerySelectDeParser getSelectDeParser()
    {
        return new QuerySelectDeParser();
    }
    
    /**
     * Provide implementation of expression deparser if the default (BaseExpressionDeParser) 
     * is not sufficient.
     * 
     * @param dep
     * @param sb
     * @return expression deparser impl
     */
    protected BaseExpressionDeParser getExpressionDeparser(SelectDeParser dep, StringBuffer sb)
    {
        return new BaseExpressionDeParser(dep, sb);
    }

    public String getSQL()
    {
        doNavigate();
        StringBuffer sb = new StringBuffer();
        SelectDeParser deParser = getSelectDeParser();
        deParser.setBuffer(sb);
        ExpressionDeParser expressionDeParser = getExpressionDeparser(deParser, sb);
        deParser.setExpressionVisitor(expressionDeParser);
        Select select = (Select) statement;
        select.getSelectBody().accept(deParser);
        return deParser.getBuffer().toString();
    }

    public List<ParamDesc> getSelectList()
    {
        doNavigate();
        return selectList;
    }

    @Override
    public String getInfo()
    {
        doNavigate();
        return queryString;
    }

}
