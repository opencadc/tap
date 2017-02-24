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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.dali.tables.ascii.AsciiTableWriter;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableParam;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.writer.ResultSetTableData;
import ca.nrc.cadc.tap.writer.RssTableWriter;
import ca.nrc.cadc.tap.writer.format.FormatFactory;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.ParameterUtil;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

public class DefaultTableWriter implements TableWriter
{

    private static final Logger log = Logger.getLogger(DefaultTableWriter.class);

    private static final String FORMAT = "RESPONSEFORMAT";
    private static final String FORMAT_ALT = "FORMAT";

    // shortcuts
    public static final String CSV = "csv";
    public static final String FITS = "fits";
    public static final String HTML = "html";
    public static final String TEXT = "text";
    public static final String TSV = "tsv";
    public static final String VOTABLE = "votable";
    public static final String RSS = "rss";

    // content-type
//    private static final String APPLICATION_FITS = "application/fits";
    private static final String APPLICATION_VOTABLE_XML = "application/x-votable+xml";
    private static final String APPLICATION_RSS = "application/rss+xml";
    private static final String TEXT_XML_VOTABLE = "text/xml;content=x-votable"; // the SIAv1 mimetype
    private static final String TEXT_CSV = "text/csv";
//    private static final String TEXT_HTML = "text/html";
//    private static final String TEXT_PLAIN = "text/plain";
    private static final String TEXT_TAB_SEPARATED_VALUES = "text/tab-separated-values";
    private static final String TEXT_XML = "text/xml";

    private static final Map<String,String> knownFormats = new TreeMap<String,String>();

    static
    {
        knownFormats.put(APPLICATION_VOTABLE_XML, VOTABLE);
        knownFormats.put(TEXT_XML, VOTABLE);
        knownFormats.put(TEXT_XML_VOTABLE, VOTABLE);
        knownFormats.put(TEXT_CSV, CSV);
        knownFormats.put(TEXT_TAB_SEPARATED_VALUES, TSV);
        knownFormats.put(VOTABLE, VOTABLE);
        knownFormats.put(CSV, CSV);
        knownFormats.put(TSV, TSV);
        knownFormats.put(RSS, RSS);
        knownFormats.put(APPLICATION_RSS, RSS);
    }

    private Job job;
    private String queryInfo;
    private String contentType;
    private String extension;

    // RssTableWriter not yet ported to cadcDALI
    private ca.nrc.cadc.dali.tables.TableWriter<VOTableDocument> tableWriter;
    private RssTableWriter rssTableWriter;
    
    private FormatFactory formatFactory;
    
    private long rowcount = 0l;

    // once the RssTableWriter is converted to use the DALI format
    // of writing, this reference will not be needed
    List<TapSelectItem> selectList;

    public DefaultTableWriter() { }

    @Override
    public void setJob(Job job)
    {
        this.job = job;
        initFormat();
    }

    @Override
    public void setSelectList(List<TapSelectItem> selectList)
    {
        this.selectList = selectList;
        if (rssTableWriter != null)
            rssTableWriter.setSelectList(selectList);
    }
    
    @Override
    public void setQueryInfo(String queryInfo)
    {
        this.queryInfo = queryInfo;
    }

    @Override
    public String getContentType()
    {
        return tableWriter.getContentType();
    }

    @Override
    public String getErrorContentType()
    {
        return tableWriter.getErrorContentType();
    }

    /**
     * Get the number of rows the output table
     * @return number of result rows written in output table
     */
    @Override
    public long getRowCount()
    {
        return rowcount;
    }
    
    @Override
    public String getExtension()
    {
        return extension;
    }
    
    private void initFormat()
    {
        String format = ParameterUtil.findParameterValue(FORMAT, job.getParameterList());
        if (format == null)
            format = ParameterUtil.findParameterValue(FORMAT_ALT, job.getParameterList());
        if (format == null)
            format = VOTABLE;
        
        String type = knownFormats.get(format.toLowerCase());
        if (type == null)
            throw new UnsupportedOperationException("unknown format: " + format);

        if (type.equals(VOTABLE) && format.equals(VOTABLE))
            format = APPLICATION_VOTABLE_XML;
        
        // Create the table writer (handle RSS the old way for now)
        // Note: This needs to be done before the write method is called so the contentType
        // can be determined from the table writer.

        if (type.equals(RSS))
        {
            rssTableWriter = new RssTableWriter();
            this.contentType = rssTableWriter.getContentType();
            rssTableWriter.setJob(job);
        }
        else
        {
            if (type.equals(VOTABLE))
                tableWriter = new VOTableWriter(format);
            if (type.equals(CSV))
                tableWriter = new AsciiTableWriter(AsciiTableWriter.ContentType.CSV);
            if (type.equals(TSV))
                tableWriter = new AsciiTableWriter(AsciiTableWriter.ContentType.TSV);

            if (tableWriter == null)
            {
                // legal format but we don't have a table writer for it
                throw new UnsupportedOperationException("unsupported format: " + type);
            }

            this.contentType = tableWriter.getContentType();
            this.extension = tableWriter.getExtension();
        }
        log.debug("created: " + tableWriter.getClass().getName());
    }

    public void setFormatFactory(FormatFactory formatFactory)
    {
        this.formatFactory = formatFactory;
    }

    @Override
    public void setFormatFactory(ca.nrc.cadc.dali.util.FormatFactory formatFactory)
    {
        throw new UnsupportedOperationException("Use custom tap format factory implementation class");
    }

    public void write(Throwable t, OutputStream out) 
        throws IOException
    {
        tableWriter.write(t, out);
    }

    
    @Override
    public void write(ResultSet rs, OutputStream out) throws IOException
    {
        this.write(rs, out, null);
    }

    @Override
    public void write(ResultSet rs, OutputStream out, Long maxrec)
            throws IOException
    {
        Writer writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
        this.write(rs, writer, maxrec);
    }

    @Override
    public void write(ResultSet rs, Writer out) throws IOException
    {
        this.write(rs, out, null);
    }

    @Override
    public void write(ResultSet rs, Writer out, Long maxrec) throws IOException
    {
        if (rs != null && log.isDebugEnabled())
            try { log.debug("resultSet column count: " + rs.getMetaData().getColumnCount()); }
            catch(Exception oops) { log.error("failed to check resultset column count", oops); }

        if (rssTableWriter != null)
        {
            rssTableWriter.setJob(job);
            rssTableWriter.setSelectList(selectList);
            rssTableWriter.setFormatFactory(formatFactory);
            rssTableWriter.setQueryInfo(queryInfo);
            if (maxrec != null)
                rssTableWriter.write(rs, out, maxrec);
            else
                rssTableWriter.write(rs, out);
            return;
        }
        
        VOTableDocument votableDocument = new VOTableDocument();

        VOTableResource resultsResource = new VOTableResource("results");
        VOTableTable resultsTable = new VOTableTable();

        // get the formats based on the selectList
        List<Format<Object>> formats = formatFactory.getFormats(selectList);

        List<String> serviceIDs = new ArrayList<String>();
        int listIndex = 0;

        // Add the metadata elements.
        for (TapSelectItem resultCol : selectList)
        {
            VOTableField newField = createVOTableField(resultCol);

            Format<Object> format = formats.get(listIndex);
            log.debug("format: " + listIndex + " " + format.getClass().getName());
            newField.setFormat(format);

            resultsTable.getFields().add(newField);

            if (newField.id != null)
            {
                if ( !serviceIDs.contains(newField.id) )
                    serviceIDs.add(newField.id);
                else
                    newField.id = null; // avoid multiple ID with same value in output
            }

            listIndex++;
        }

        resultsResource.setTable(resultsTable);
        votableDocument.getResources().add(resultsResource);

        // Add the "meta" resources to describe services for each columnID in
        // list columnIDs that we recognize
        addMetaResources(votableDocument, serviceIDs);

        ResultSetTableData tableData = new ResultSetTableData(rs, formats);

        VOTableInfo info = new VOTableInfo("QUERY_STATUS", "OK");
        resultsResource.getInfos().add(info);

        // for documentation, add the query to the table as an info element
        if (queryInfo != null)
        {
            info = new VOTableInfo("QUERY", queryInfo);
            resultsResource.getInfos().add(info);
        }

        resultsTable.setTableData(tableData);
        
        if (maxrec != null)
            tableWriter.write(votableDocument, out, maxrec);
        else
            tableWriter.write(votableDocument, out);
        
        this.rowcount = tableData.getRowCount();
    }

    private void addMetaResources(VOTableDocument votableDocument, List<String> serviceIDs)
        throws IOException
    {
        for (String serviceID : serviceIDs)
        {
            String filename = serviceID + ".xml";
            InputStream is = DefaultTableWriter.class.getClassLoader().getResourceAsStream(filename);
            if (is == null)
            {
                //throw new MissingResourceException(
                //    "Resource not found: " + serviceID + ".xml", DefaultTableWriter.class.getName(), filename);
                log.debug("failed to find service resource " + filename + " to go with XML ID " + serviceID);
            }
            else
            {
                VOTableReader reader = new VOTableReader();
                VOTableDocument serviceDocument = reader.read(is);
                VOTableResource metaResource = serviceDocument.getResourceByType("meta");
                votableDocument.getResources().add(metaResource);

                // set the access URL from resourceIdentifier if possible
                RegistryClient regClient = new RegistryClient();

                try
                {
                    URI resourceIdentifier = null;
                    URI standardID = null;
                    Iterator<VOTableParam> i = metaResource.getParams().iterator();
                    while ( i.hasNext() )
                    {
                        VOTableParam vp = i.next();
                        if (vp.getName().equals("resourceIdentifier"))
                        {
                            resourceIdentifier = new URI(vp.getValue());
                        }
                        else if (vp.getName().equals("standardID"))
                        {
                            standardID = new URI(vp.getValue());
                        }
                    }
                    if (resourceIdentifier != null)
                    {
                        Subject s = AuthenticationUtil.getCurrentSubject();
                        AuthMethod cur = AuthenticationUtil.getAuthMethod(s);
                        if (cur == null)
                        {
                            cur = AuthMethod.ANON;
                        }
                        log.debug("resourceIdentifier=" + resourceIdentifier + ", standardID=" + standardID + ", authMethod=" + cur);
                        URL accessURL = regClient.getServiceURL(resourceIdentifier, standardID, cur);
                        String surl = accessURL.toExternalForm();
                        VOTableParam accessParam = new VOTableParam("accessURL", "char", surl.length(), false, surl);
                        metaResource.getParams().add(accessParam);
                    }
                }
                catch (URISyntaxException e)
                {
                    throw new RuntimeException("resourceIdentifier in " + filename + " is invalid", e);
                }
            }
        }
    }

    protected VOTableField createVOTableField(TapSelectItem resultCol)
    {
        if (resultCol != null)
        {
            TapDataType tt = resultCol.getDatatype();
            VOTableField newField = new VOTableField(resultCol.getName(),
                    tt.getDatatype(), tt.arraysize, tt.varSize, null);
            newField.xtype = tt.xtype;
            newField.description = resultCol.description;
            newField.id = resultCol.id;
            newField.utype = resultCol.utype;
            newField.ucd = resultCol.ucd;
            newField.unit = resultCol.unit;

            return newField;
        }

        return null;
    }

    /*
    private String getParamName(TapSelectItem paramDesc)
    {
        String name = paramDesc.name;
        String alias = paramDesc.alias;
        if (alias != null)
        {
            // strip off double-quotes used for an alias with spaces or dots in it
            if (alias.charAt(0) == '"' && alias.charAt(alias.length()-1) == '"')
                alias = alias.substring(1, alias.length() - 1);
            return alias;
        }
        else if (name != null)
            return name;

        return null;
    }
    */
    
    /*
    private String getDatatype(TapSelectItem paramDesc)
    {
        String datatype = paramDesc.datatype;

        if (datatype == null)
            return null;

        if (datatype.equals("adql:SMALLINT"))
        {
            return "short";
        }
        else if (datatype.equals("adql:INTEGER"))
        {
            return "int";
        }
        else if (datatype.equals("adql:BIGINT"))
        {
            return "long";
        }
        else if (datatype.equals("adql:REAL") || datatype.equals("adql:FLOAT"))
        {
            return "float";
        }
        else if (datatype.equals("adql:DOUBLE") )
        {
            return "double";
        }
        else if (datatype.equals("adql:VARBINARY"))
        {
            return "unsignedByte";
        }
        else if (datatype.equals("adql:CHAR"))
        {
            return "char";
        }
        else if (datatype.equals("adql:VARCHAR"))
        {
            return "char";
        }
        else if (datatype.equals("adql:BINARY"))
        {
            return "unsignedByte";
        }
        else if (datatype.equals("adql:BLOB"))
        {
            return "unsignedByte";
        }
        else if (datatype.equals("adql:CLOB"))
        {
            return "char";
        }
        else if (datatype.equals("adql:TIMESTAMP"))
        {
            return "char";
        }
        else if (datatype.equals("adql:POINT"))
        {
            return "char";
        }
        else if (datatype.equals("adql:CIRCLE"))
        {
            return "char";
        }
        else if (datatype.equals("adql:POLYGON"))
        {
            return "char";
        }
        else if (datatype.equals("adql:REGION"))
        {
            return "char";
        }
        else if (datatype.equals("adql:proto:INTERVAL"))
        {
            return "double";
        }
        // here we support votable datatypes used directly in the tap_schema,
        // which are normally only needed if the DB has arrays of primitive types
        // as adql types cover all the other scenarios
        else if (datatype.equals("votable:double"))
        {
            return "double";
        }
        else if (datatype.equals("votable:int"))
        {
            return "int";
        }
        else if (datatype.equals("votable:float"))
        {
            return "float";
        }
        else if (datatype.equals("votable:long"))
        {
            return "long";
        }
        else if (datatype.equals("votable:boolean"))
        {
            return "boolean";
        }
        else if (datatype.equals("votable:short"))
        {
            return "short";
        }
        else if (datatype.equals("votable:char"))
        {
            return "char";
        }
        else if (datatype.equals("votable:char*"))
        {
            return "char";
        }
        
        if (datatype.equals("point"))
        {
            return "double";
        }
        else if (datatype.equals("circle"))
        {
            return "double";
        }
        else if (datatype.equals("polygon"))
        {
            return "double";
        }
        else if (datatype.equals("interval"))
        {
            return "double";
        }
        else if (datatype.equals("uuid"))
        {
            return "char";
        }

        return null;
    }
    */

    /*
    private void setSize(TapSelectItem paramDesc, VOTableField field)
    {
        String datatype = paramDesc.getDatatype();
        Integer size = paramDesc.getArraysize();

        if (datatype == null)
            return;

        if (datatype.equals("adql:BINARY"))
        {
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("adql:VARBINARY"))
        {
            field.setVariableSize(true);
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("adql:CHAR"))
        {
            //field.setVariableSize(true);
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("adql:VARCHAR"))
        {
            field.setVariableSize(true);
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("adql:BLOB"))
        {
            field.setVariableSize(true);
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("adql:CLOB"))
        {
            field.setVariableSize(true);
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("adql:TIMESTAMP"))
        {
            field.setVariableSize(true);
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("adql:POINT"))
        {
            field.setVariableSize(true);
        }
        else if (datatype.equals("adql:CIRCLE"))
        {
            field.setVariableSize(true);
        }
        else if (datatype.equals("adql:POLYGON"))
        {
            field.setVariableSize(true);
        }
        else if (datatype.equals("adql:REGION"))
        {
            field.setVariableSize(true);
        }
        else if (datatype.equals("adql:proto:INTERVAL"))
        {
            field.setVariableSize(true);
        }
        // VOTable types
        else if (datatype.equals("votable:double"))
        {
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("votable:int"))
        {
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("votable:float"))
        {
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("votable:long"))
        {
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("votable:boolean"))
        {
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("votable:short"))
        {
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("votable:char"))
        {
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("votable:char*"))
        {
            field.setVariableSize(true);
        }
        // DALI-1.1 xtypes
        else if (datatype.equals("point"))
        {
            field.setArraysize(2);
        }
        else if (datatype.equals("circle"))
        {
            field.setArraysize(3);
        }
        else if (datatype.equals("polygon"))
        {
            field.setVariableSize(true);
        }
        else if (datatype.equals("interval"))
        {
            field.setArraysize(2);
        }
        else if (datatype.equals("uuid"))
        {
            field.setArraysize(36);
        }
    }
    */
}
