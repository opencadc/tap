package ca.nrc.cadc.tap;

import ca.nrc.cadc.dali.tables.TableData;
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
import ca.nrc.cadc.tap.schema.ParamDesc;
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
import java.util.MissingResourceException;
import java.util.TreeMap;
import org.apache.log4j.Logger;

public class DefaultTableWriter implements TableWriter
{

    private static final Logger log = Logger.getLogger(DefaultTableWriter.class);

    private static final String FORMAT = "FORMAT";

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

    // once the RssTableWriter is converted to use the DALI format
    // of writing, this reference will not be needed
    List<ParamDesc> selectList;

    public DefaultTableWriter() { }

    public void setJob(Job job)
    {
        this.job = job;
        initFormat();
    }

    public void setSelectList(List<ParamDesc> selectList)
    {
        this.selectList = selectList;
        if (rssTableWriter != null)
            rssTableWriter.setSelectList(selectList);
    }
    
    public void setQueryInfo(String queryInfo)
    {
        this.queryInfo = queryInfo;
    }

    @Override
    public String getContentType()
    {
        return contentType;
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
        for (ParamDesc paramDesc : selectList)
        {
            VOTableField newField = createVOTableField(paramDesc);

            Format<Object> format = formats.get(listIndex);
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

        TableData tableData = new ResultSetTableData(rs, formats);

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
                throw new MissingResourceException(
                    "Resource not found: " + serviceID + ".xml", DefaultTableWriter.class.getName(), filename);
            }
            VOTableReader reader = new VOTableReader();
            VOTableDocument serviceDocument = reader.read(is);
            VOTableResource metaResource = serviceDocument.getResourceByType("meta");
            votableDocument.getResources().add(metaResource);

            // set the access URL from resourceIdentifier if possible
            RegistryClient regClient = new RegistryClient();
            
            try
            {
                URI resourceIdentifier = null;
                Iterator<VOTableParam> i = metaResource.getParams().iterator();
                while ( i.hasNext() )
                {
                    VOTableParam vp = i.next();
                    if (vp.getName().equals("resourceIdentifier"))
                    {
                        resourceIdentifier = new URI(vp.getValue());
                    }
                }
                if (resourceIdentifier != null)
                {
                    URL accessURL = regClient.getServiceURL(resourceIdentifier);
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

    protected VOTableField createVOTableField(ParamDesc paramDesc)
    {
        if (paramDesc != null)
        {
            String name = getParamName(paramDesc);
            String datatype = getDatatype(paramDesc);

            VOTableField newField = new VOTableField(name, datatype);

            setSize(paramDesc, newField);

            if (paramDesc.id != null)
                newField.id = paramDesc.id; // an XML id

            if (paramDesc.columnDesc != null)
                newField.utype = paramDesc.columnDesc.utype;
            else
                newField.utype = paramDesc.utype;

            newField.ucd = paramDesc.ucd;
            newField.unit = paramDesc.unit;

            if (paramDesc.datatype != null && paramDesc.datatype.startsWith("adql:"))
                newField.xtype = paramDesc.datatype;

            newField.description = paramDesc.description;

            return newField;
        }

        return null;
    }

    // Set the name using the alias first, then the column name.
    /**
     *
     * @param alias
     * @param name
     */
    private String getParamName(ParamDesc paramDesc)
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

    /**
     *
     * @param datatype
     * @param size
     */
    private String getDatatype(ParamDesc paramDesc)
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

        return null;
    }

    /**
     *
     * @param datatype
     * @param size
     */
    private void setSize(ParamDesc paramDesc, VOTableField field)
    {
        String datatype = paramDesc.datatype;
        Integer size = paramDesc.size;

        if (datatype == null)
            return;

        if (datatype.equals("adql:VARBINARY"))
        {
            field.setVariableSize(true);
        }
        else if (datatype.equals("adql:CHAR"))
        {
            field.setVariableSize(true);
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("adql:VARCHAR"))
        {
            field.setVariableSize(true);
            if (size != null)
                field.setArraysize(size);
        }
        else if (datatype.equals("adql:BINARY"))
        {
            if (size == null)
                field.setVariableSize(true);
            else
                field.setArraysize(size);
        }
        else if (datatype.equals("adql:BLOB"))
        {
            if (size == null)
                field.setVariableSize(true);
            else
                field.setArraysize(size);
        }
        else if (datatype.equals("adql:CLOB"))
        {
            if (size == null)
                field.setVariableSize(true);
            else
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
        // here we support votable datatypes used directly in the tap_schema,
        // which are normally only needed if the DB has arrays of primitive types
        // as adql types cover all the other scenarios
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
    }


}
