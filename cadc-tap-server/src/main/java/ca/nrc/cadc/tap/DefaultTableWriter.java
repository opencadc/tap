/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.writer.ResultSetTableData;
import ca.nrc.cadc.tap.writer.RssTableWriter;
import ca.nrc.cadc.tap.writer.format.FormatFactory;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.ParameterUtil;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

public class DefaultTableWriter implements TableWriter {

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

    // content-types
    private static final String APPLICATION_VOTABLE_XML = "application/x-votable+xml";
    private static final String APPLICATION_RSS = "application/rss+xml";
    private static final String TEXT_XML_VOTABLE = "text/xml;content=x-votable"; // the SIAv1 mimetype
    private static final String TEXT_CSV = "text/csv";
    private static final String TEXT_TAB_SEPARATED_VALUES = "text/tab-separated-values";
    private static final String TEXT_XML = "text/xml";
    private static final String APPLICATION_PARQUET = "application/vnd.apache.parquet";
    private static final String PARQUET = "parquet";

    private static final String PARQUET_CLASS_NAME = "ca.nrc.cadc.dali.tables.parquet.ParquetWriter";

    private static final Map<String, String> knownFormats = new TreeMap<>();

    static {
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
        try {
            // optional plugin
            Class clz = Class.forName(PARQUET_CLASS_NAME);
            knownFormats.put(APPLICATION_PARQUET, PARQUET);
            knownFormats.put(PARQUET, PARQUET);
        } catch (ClassNotFoundException ex) {
            log.debug("class not found: ca.nrc.cadc.dali.tables.parquet.ParquetWriter - no parquet output");
        }
    }

    private Job job;
    private String queryInfo;
    private String contentType;
    private String extension;

    // RssTableWriter not yet ported to cadcDALI
    private ca.nrc.cadc.dali.tables.TableWriter<VOTableDocument> tableWriter;
    private RssTableWriter rssTableWriter;

    private FormatFactory formatFactory;
    private boolean errorWriter = false;

    private long rowcount = 0L;

    // once the RssTableWriter is converted to use the DALI format
    // of writing, this reference will not be needed
    protected final List<TapSelectItem> selectList = new ArrayList<>();

    public DefaultTableWriter() {
        this(false);
    }

    public DefaultTableWriter(boolean errorWriter) {
        this.errorWriter = errorWriter;
    }

    @Override
    public void setJob(Job job) {
        this.job = job;
        initFormat();
    }

    @Override
    public void setSelectList(List<TapSelectItem> selectList) {
        this.selectList.addAll(selectList);
        if (rssTableWriter != null) {
            rssTableWriter.setSelectList(selectList);
        }
    }

    @Override
    public void setQueryInfo(String queryInfo) {
        this.queryInfo = queryInfo;
    }

    @Override
    public String getContentType() {
        return tableWriter.getContentType();
    }

    @Override
    public String getErrorContentType() {
        return tableWriter.getErrorContentType();
    }

    /**
     * Get the number of rows the output table
     *
     * @return number of result rows written in output table
     */
    @Override
    public long getRowCount() {
        return rowcount;
    }

    @Override
    public String getExtension() {
        return extension;
    }

    private void initFormat() {
        String format = ParameterUtil.findParameterValue(FORMAT, job.getParameterList());
        if (format == null) {
            format = ParameterUtil.findParameterValue(FORMAT_ALT, job.getParameterList());
        }
        if (format == null) {
            format = VOTABLE;
        }
        String type = knownFormats.get(format.toLowerCase());
        if (type == null && errorWriter) {
            type = VOTABLE;
            format = VOTABLE;
        } else if (type == null) {
            throw new UnsupportedOperationException("unknown format: " + format);
        }

        if (type.equals(VOTABLE) && format.equals(VOTABLE)) {
            format = APPLICATION_VOTABLE_XML;
        }
        // Create the table writer (handle RSS the old way for now)
        // Note: This needs to be done before the write method is called so the contentType
        // can be determined from the table writer.

        if (type.equals(RSS)) {
            rssTableWriter = new RssTableWriter();
            rssTableWriter.setJob(job);
            // for error handling
            tableWriter = new AsciiTableWriter(AsciiTableWriter.ContentType.TSV);
        } else if (type.equals(VOTABLE)) {
            tableWriter = new VOTableWriter(format);
        } else if (type.equals(CSV)) {
            tableWriter = new AsciiTableWriter(AsciiTableWriter.ContentType.CSV);
        } else if (type.equals(TSV)) {
            tableWriter = new AsciiTableWriter(AsciiTableWriter.ContentType.TSV);
        } else if (type.equals(PARQUET)) {
            try {
                Class clz = Class.forName(PARQUET_CLASS_NAME);
                tableWriter = (ca.nrc.cadc.dali.tables.TableWriter<VOTableDocument>) clz.getConstructor().newInstance();
            } catch (Exception ex) {
                log.warn("caller requested " + PARQUET + " output but failed to load ParquetWriter");
            }
        }

        if (tableWriter == null) {
            throw new UnsupportedOperationException("unsupported format: " + type);
        }
        this.contentType = tableWriter.getContentType();
        this.extension = tableWriter.getExtension();
    }

    @Override
    public void setFormatFactory(FormatFactory formatFactory) {
        this.formatFactory = formatFactory;
    }

    @Override
    public void setFormatFactory(ca.nrc.cadc.dali.util.FormatFactory formatFactory) {
        throw new UnsupportedOperationException("Use custom tap format factory implementation class");
    }

    public void write(Throwable t, OutputStream out)
            throws IOException {
        tableWriter.write(t, out);
    }

    @Override
    public void write(ResultSet rs, OutputStream out) throws IOException {
        this.write(rs, out, null);
    }

    @Override
    public void write(ResultSet rs, OutputStream out, Long maxrec) throws IOException {
        if (rs != null && log.isDebugEnabled()) {
            try {
                log.debug("resultSet column count: " + rs.getMetaData().getColumnCount());
            } catch (Exception oops) {
                log.error("failed to check resultset column count", oops);
            }
        }

        // HACK: delegate everything to an alternate TAP TableWriter
        if (rssTableWriter != null) {
            rssTableWriter.setJob(job);
            rssTableWriter.setSelectList(selectList);
            rssTableWriter.setFormatFactory(formatFactory);
            rssTableWriter.setQueryInfo(queryInfo);
            if (maxrec != null) {
                rssTableWriter.write(rs, out, maxrec);
            } else {
                rssTableWriter.write(rs, out);
            }
            return;
        }

        VOTableDocument votableDocument = generateOutputTable();
        VOTableTable resultsTable = votableDocument.getResourceByType("results").getTable();

        // get the formats based on the selectList; some of these are ResultSetFormat
        // and used by ResultSetTableData to extract from JDBC
        List<Format<Object>> formats = formatFactory.getFormats(selectList);
        for (int i = 0; i < formats.size(); i++) {
            // attach format object to field so the tableWriter can use 
            // Format.format(Object) or other future methods
            resultsTable.getFields().get(i).setFormat(formats.get(i));
        }

        // attach dynamic table data
        ResultSetTableData tableData = new ResultSetTableData(rs, formats);
        resultsTable.setTableData(tableData);

        if (maxrec != null) {
            tableWriter.write(votableDocument, out, maxrec);
        } else {
            tableWriter.write(votableDocument, out);
        }

        this.rowcount = tableData.getRowCount();
    }
    
    @Override
    public VOTableDocument generateOutputTable() throws IOException {
        if (rssTableWriter != null) {
            return null; // ugh
        }

        VOTableDocument votableDocument = new VOTableDocument();

        VOTableResource resultsResource = new VOTableResource("results");
        VOTableTable resultsTable = new VOTableTable();
        resultsResource.setTable(resultsTable);
        votableDocument.getResources().add(resultsResource);

        // extract select list field ID values
        List<String> serviceIDs = new ArrayList<>();
        for (TapSelectItem resultCol : selectList) {
            VOTableField newField = createVOTableField(resultCol);
            resultsTable.getFields().add(newField);
            if (newField.id != null) {
                if (!serviceIDs.contains(newField.id)) {
                    serviceIDs.add(newField.id);
                } else {
                    // avoid multiple ID with same value in output
                    // e.g. duplicate columns from a join
                    newField.id = null;
                }
            }
        }
        // add meta resources aka service descriptors for selected IDs
        addMetaResources(votableDocument, serviceIDs);

        VOTableInfo info = new VOTableInfo("QUERY_STATUS", "OK");
        resultsResource.getInfos().add(info);

        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        Date now = new Date();
        VOTableInfo info2 = new VOTableInfo("QUERY_TIMESTAMP", df.format(now));
        resultsResource.getInfos().add(info2);

        // for documentation, add the query to the table as an info element
        if (queryInfo != null) {
            info = new VOTableInfo("QUERY", queryInfo);
            resultsResource.getInfos().add(info);
        }
        
        return votableDocument;
    }

    // HACK: to allow an ObsCore.access_url formatter to access this info
    //       but this is tightly coupled to the tap_schema.columns.column_id
    //       matching a config file named {column_id}.xml and hopefully containing a
    //       service descriptor with a DataLink standardID.... TBD.
    public static URL getAccessURL(String columnID, URI reqStandardID) throws IOException {
        VOTableDocument serviceDocument = getDoc(columnID);
        if (serviceDocument == null) {
            return null;
        }

        String filename = columnID + ".xml"; // for error reporting
        // find specified endpoint
        for (VOTableResource metaResource : serviceDocument.getResources()) {
            if ("meta".equals(metaResource.getType())) {
                try {
                    URL accessURL = null;
                    URI resourceIdentifier = null;
                    URI standardID = null;
                    Iterator<VOTableParam> i = metaResource.getParams().iterator();
                    while (i.hasNext()) {
                        VOTableParam vp = i.next();
                        if (vp.getName().equals("accessURL")) {
                            accessURL = new URL(vp.getValue());
                        } else if (vp.getName().equals("resourceIdentifier")) {
                            resourceIdentifier = new URI(vp.getValue());
                        } else if (vp.getName().equals("standardID")) {
                            standardID = new URI(vp.getValue());
                        }
                    }
                    log.debug("getAccessURL: " + reqStandardID + " vs " + standardID);
                    if (reqStandardID.equals(standardID)) {
                        if (accessURL == null && resourceIdentifier != null && standardID != null) {
                            // try to augment resource with accessURL
                            Subject s = AuthenticationUtil.getCurrentSubject();
                            AuthMethod cur = AuthenticationUtil.getAuthMethod(s);
                            if (cur == null) {
                                cur = AuthMethod.ANON;
                            }
                            RegistryClient regClient = new RegistryClient();
                            log.debug("resourceIdentifier=" + resourceIdentifier + ", standardID=" + standardID + ", authMethod=" + cur);
                            accessURL = regClient.getServiceURL(resourceIdentifier, standardID, cur);
                        }
                        if (accessURL != null) {
                            return accessURL;
                        }
                    }
                } catch (URISyntaxException e) {
                    throw new RuntimeException("CONFIG: URI in " + filename + " is invalid", e);
                }
            }
        }

        return null;
    }

    // read a votable document matching a {column_id}: currently in the config dir
    private static VOTableDocument getDoc(String sid) throws IOException {
        File configDir = new File(System.getProperty("user.home") + "/config");
        String filename = sid + ".xml";
        File tmpl = new File(configDir, filename);
        Reader rdr = null;
        if (tmpl.exists()) {
            rdr = new FileReader(tmpl);
        } else {
            // backwards compat: classpath
            InputStream is = DefaultTableWriter.class.getClassLoader().getResourceAsStream(filename);
            if (is != null) {
                rdr = new InputStreamReader(is);
            }
        }
        if (rdr == null) {
            log.debug("failed to find config resource " + filename + " to go with XML ID " + sid);
            return null;
        }

        VOTableReader reader = new VOTableReader();
        VOTableDocument serviceDocument = reader.read(rdr);
        return serviceDocument;
    }

    /**
     * Optionally add meta resources to the VOTableDocument. These are expected to be
     * DataLink service descriptors that go with the columns (fields) in the select list.
     * Normally, fields get an ID if one is assigned in the tap_schema metadata. The default
     * implementation uses the <code>fieldIDs</code> valeus to find service descriptor templates,
     * optionally process them to add the accessURL, and add them to the document. 
     * Find is currently: look for a file named {fieldID}.xml in the {user.home}/config
     * directory.
     * 
     * @param votableDocument the document to add meta resources to
     * @param fieldIDs list of FIELD ID attributes for items in the select list
     * @throws IOException if failing to read files from the config dir
     */
    protected void addMetaResources(VOTableDocument votableDocument, List<String> fieldIDs)
            throws IOException {
        RegistryClient regClient = new RegistryClient();
        for (String fid : fieldIDs) {
            VOTableDocument serviceDocument = getDoc(fid);
            String filename = fid + ".xml";
            if (serviceDocument == null) {
                return;
            }

            for (VOTableResource metaResource : serviceDocument.getResources()) {
                if ("meta".equals(metaResource.getType())) {
                    votableDocument.getResources().add(metaResource);
                    try {
                        URL accessURL = null;
                        URI resourceIdentifier = null;
                        URI standardID = null;
                        Iterator<VOTableParam> i = metaResource.getParams().iterator();
                        while (i.hasNext()) {
                            VOTableParam vp = i.next();
                            if (vp.getName().equals("accessURL")) {
                                accessURL = new URL(vp.getValue());
                            } else if (vp.getName().equals("resourceIdentifier")) {
                                resourceIdentifier = new URI(vp.getValue());
                            } else if (vp.getName().equals("standardID")) {
                                standardID = new URI(vp.getValue());
                            }
                        }
                        if (accessURL == null && resourceIdentifier != null && standardID != null) {
                            // try to augment resource with accessURL
                            Subject s = AuthenticationUtil.getCurrentSubject();
                            AuthMethod cur = AuthenticationUtil.getAuthMethod(s);
                            if (cur == null) {
                                cur = AuthMethod.ANON;
                            }
                            log.debug("resourceIdentifier=" + resourceIdentifier + ", standardID=" + standardID + ", authMethod=" + cur);
                            accessURL = regClient.getServiceURL(resourceIdentifier, standardID, cur);
                            if (accessURL != null) {
                                String surl = accessURL.toExternalForm();
                                String arraysize = Integer.toString(surl.length()); // fixed length since we know it
                                VOTableParam accessParam = new VOTableParam("accessURL", "char", arraysize, surl);
                                metaResource.getParams().add(accessParam);
                            } else {
                                // log the error but continue anyway
                                log.error("failed to find accessURL: resourceIdentifier=" + resourceIdentifier
                                        + ", standardID=" + standardID + ", authMethod=" + cur);
                            }
                        }
                    } catch (URISyntaxException e) {
                        throw new RuntimeException("resourceIdentifier in " + filename + " is invalid", e);
                    }
                }
            }
        }
    }

    protected VOTableField createVOTableField(TapSelectItem resultCol) {
        if (resultCol != null) {
            TapDataType tt = resultCol.getDatatype();
            VOTableField newField = new VOTableField(resultCol.getName(), tt.getDatatype(), tt.arraysize);
            newField.xtype = tt.xtype;
            newField.description = resultCol.description;
            newField.id = resultCol.columnID;
            newField.utype = resultCol.utype;
            newField.ucd = resultCol.ucd;
            newField.unit = resultCol.unit;

            return newField;
        }

        return null;
    }
}
