/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2014.                            (c) 2014.
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

package ca.nrc.cadc.tap;

import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.QuerySelectDeParser;
import ca.nrc.cadc.tap.parser.converter.TableNameConverter;
import ca.nrc.cadc.tap.parser.extractor.SelectListExpressionExtractor;
import ca.nrc.cadc.tap.parser.extractor.SelectListExtractor;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import ca.nrc.cadc.tap.schema.ParamDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.uws.Job;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.apache.log4j.Logger;

import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author pdowler
 */
public abstract class AbstractTapQuery implements TapQuery
{
    private static Logger log = Logger.getLogger(AbstractTapQuery.class);

    protected Job job;
    protected TapSchema tapSchema;
    protected Statement statement;
    protected Map<String, TableDesc> extraTables;
    protected Integer maxRowCount;
    
    protected AbstractTapQuery() { }

    public void setJob(Job job)
    {
        this.job = job;
    }
    
    public void setTapSchema(TapSchema tapSchema)
    {
        this.tapSchema = tapSchema;
    }

    public void setExtraTables(Map<String, TableDesc> extraTables)
    {
        this.extraTables = extraTables;
    }

    public void setMaxRowCount(Integer maxRowCount)
    {
        this.maxRowCount = maxRowCount;
    }

    public abstract List<ParamDesc> getSelectList();

    abstract void doNavigate();

    abstract ExpressionDeParser getExpressionDeparser(
            final SelectDeParser selectDeParser,
            final StringBuffer stringBuffer);

    /**
     * Run all navigators on a statement.
     *
     * @param statement     The statement to navigate.
     */
    List<ParamDesc> navigateStatement(final Statement statement,
                                      final List<SelectNavigator> navigatorList)
    {
        final List<ParamDesc> selectList = new ArrayList<ParamDesc>();

        for (final SelectNavigator sn : navigatorList)
        {
            log.debug("Navigated by: " + sn.getClass().getName());

            ParserUtil.parseStatement(statement, sn);

            if (sn instanceof SelectListExtractor)
            {
                final SelectListExpressionExtractor slen =
                        (SelectListExpressionExtractor) sn.getExpressionNavigator();
                selectList.addAll(slen.getSelectList());
            }
        }

        return selectList;
    }

    /**
     * Provide implementation of select deparser if the default (SelectDeParser) is not sufficient.
     *
     * @return  QuerySelectDeParser  instance.
     */
    protected QuerySelectDeParser getSelectDeParser()
    {
        return new QuerySelectDeParser();
    }

    void appendExtraTablesNavigator(final List<SelectNavigator> navigatorList,
                                    final ExpressionNavigator expressionNavigator,
                                    final ReferenceNavigator referenceNavigator)
    {
        // support for file uploads to map the upload table name to the query table name.
        if (extraTables != null && !extraTables.isEmpty())
        {
            TableNameConverter tnc = new TableNameConverter(true);
            Set<Map.Entry<String, TableDesc>> entries = extraTables.entrySet();
            for (Map.Entry entry : entries)
            {
                String newName = (String) entry.getKey();
                TableDesc tableDesc = (TableDesc) entry.getValue();
                tnc.put(tableDesc.tableName, newName);
                log.debug("TableNameConverter " + tableDesc.tableName + " -> "
                          + newName);
            }

            navigatorList.add(new SelectNavigator(expressionNavigator,
                                                  referenceNavigator, tnc));
        }
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

    /**
     * The default implementation returns null.
     * 
     * @return null
     */
    public String getInfo()
    {
        return null;
    }
}
