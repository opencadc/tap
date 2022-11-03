/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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
************************************************************************
*/

package org.opencadc.tap.tmp;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.dali.tables.TableWriter;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.tap.ResultStore;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.ParameterUtil;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.uws.web.UWSInlineContentHandler;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.ResultSet;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

/**
 * Implementation of ResultStore and UWSINlineCOntentHandler that uses an external
 * HTTP service.
 * 
 * @author pdowler
 */
public class HttpStorageManager implements ResultStore, UWSInlineContentHandler {
    private static final Logger log = Logger.getLogger(HttpStorageManager.class);

    private static final String CONFIG = "cadc-tap-tmp.properties";
    private static final String BASE_URL_KEY = HttpStorageManager.class.getName() + ".baseURL";
    private static final String CERT_KEY = HttpStorageManager.class.getName() + ".certificate";
    
    private Job job;
    
    private String contentType;
    private String  filename;
    
    private final URL baseURL;
    private final File certFile;
    
    public HttpStorageManager() throws InvalidConfigException {
        PropertiesReader r = new PropertiesReader(CONFIG);
        MultiValuedProperties props = r.getAllProperties();
        String surl = props.getFirstPropertyValue(BASE_URL_KEY);
        try {
            this.baseURL = new URL(surl);
        } catch (MalformedURLException ex) {
            throw new InvalidConfigException("invalid " + BASE_URL_KEY + " = " + surl, ex);
        }
        String cfilename = props.getFirstPropertyValue(CERT_KEY);
        String absCertFile = System.getProperty("user.home") + "/config/" + cfilename;
        log.warn("cert file: " + absCertFile);
        this.certFile = new File(absCertFile);
    }

    @Override
    public void setJob(Job job) {
        this.job = job;
    }
    
    @Override
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public void setFilename(String filename) {
        this.filename = filename;
    }
    
    @Override
    public URL put(ResultSet rs, TableWriter<ResultSet> writer) throws IOException {
        return put(new StreamingTableWriter(rs, writer, null));
    }

    @Override
    public URL put(ResultSet rs, TableWriter<ResultSet> writer, Integer maxrec) throws IOException {
        return put(new StreamingTableWriter(rs, writer, maxrec));

    }

    @Override
    public URL put(Throwable t, TableWriter writer) throws IOException {
        return put(new StreamingTableWriter(t, writer));
    }

    private URL put(StreamingTableWriter stw)
            throws IOException {
        if (contentType == null) {
            throw new IllegalArgumentException("ContentType can not be null");
        }
        if (filename == null) {
            throw new IllegalArgumentException("filename can not be null");
        }

        //List<Parameter> paramList = job.getParameterList();
        //String resultDestination = ParameterUtil.findParameterValue("DEST", paramList);
        // TODO: output to DEST using caller credentials
        
        URL putURL = new URL(baseURL + "/" + filename);

        log.debug("put: " + putURL);
        log.debug("contentType: " + contentType);

        HttpUpload put = new HttpUpload(stw, putURL);
        put.setRequestProperty("content-type", contentType);
        Subject s = SSLUtil.createSubject(certFile);
        Subject.doAs(s, new RunnableAction(put));
        if (put.getThrowable() != null) {
            throw new RuntimeException("failed to store file " + filename, put.getThrowable());
        }
        
        return putURL;
    }
    

    @Override
    public Content accept(String name, String contentType, InputStream inputStream) 
            throws InlineContentException, IOException, ResourceNotFoundException, TransientException {
        // store the file in tmp storage
        log.debug("name: " + name);
        log.debug("Content-Type: " + contentType);
        if (inputStream == null) {
            throw new IOException("InputStream cannot be null");
        }

        String dest = name + "-" + getRandomString();
        URL putURL = new URL(baseURL + "/" + dest);

        log.debug("put: " + putURL);
        log.debug("contentType: " + contentType);

        HttpUpload put = new HttpUpload(inputStream, putURL);
        if (contentType != null) {
            put.setRequestProperty("content-type", contentType);
        }
        Subject s = SSLUtil.createSubject(certFile);
        Subject.doAs(s, new RunnableAction(put));
        
        Content ret = new Content();
        ret.name = UWSInlineContentHandler.CONTENT_PARAM_REPLACE;
        ret.value = new UWSInlineContentHandler.ParameterReplacement("param:" + name, putURL.toExternalForm());
        return ret;
    }
    
    private static String getRandomString() {
        return new RandomStringGenerator(16).getID();
    }
}
