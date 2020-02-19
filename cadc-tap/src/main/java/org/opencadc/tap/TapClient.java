/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2020.                            (c) 2020.
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

package org.opencadc.tap;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.dali.util.FormatFactory;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.net.ExpectationFailedException;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

/**
 * Basic client for Table Access Protocol (TAP).
 * 
 * @author pdowler
 * @param <E> the type of object this instance returns from query execution
 */
public class TapClient<E> {
    private static final Logger log = Logger.getLogger(TapClient.class);

    private static final String QUERY_STATUS = "QUERY_STATUS";
    private static final String QUERY_STATUS_OK = "OK";
    private static final String QUERY_STATUS_ERROR = "ERROR";
    private static final String QUERY_STATUS_OVERFLOW = "OVERFLOW";
    
    private final URI resourceID;
    private final Capabilities caps;
    private final Capability tap;
    
    /**
     * Constructor.
     * 
     * @param resourceID unique identifier for the TAP service
     * @throws ResourceNotFoundException if the resourceID lookup fails or does not include a TAP capability
     */
    public TapClient(URI resourceID) throws ResourceNotFoundException {
        this.resourceID = resourceID;
        RegistryClient reg = new RegistryClient();
        try {
            this.caps = reg.getCapabilities(resourceID);
            if (caps == null) {
                throw new ResourceNotFoundException("not found: " + resourceID);
            }
            log.debug("found capabilities: " + resourceID);
            this.tap = caps.findCapability(Standards.TAP_10);
            if (tap == null) {
                throw new ResourceNotFoundException("not found: " + Standards.TAP_10 + " capability in " + resourceID);
            }
            
        } catch (IOException ex) {
            throw new ResourceNotFoundException("not found: " + resourceID, ex);
        }
    }
    
    /**
     * Generate a usable async endpoint URL. This method only considers the specified
     * authentication method when performing the lookup of the base URL.
     * 
     * @param securityMethod IVOA securityMethod identifier
     * @return async URL
     * @throws ResourceNotFoundException if base URL matching specified auth not found
     */
    public URL getAsyncURL(URI securityMethod) 
            throws ResourceNotFoundException {
        Interface i = tap.findInterface(securityMethod);
        if (i == null) {
            throw new ResourceNotFoundException("not found: " + securityMethod + " in " + resourceID + "::" + tap.getStandardID());
        }
        String base = i.getAccessURL().getURL().toExternalForm();
        try {
            return new URL(base + "/async");
        } catch (MalformedURLException ex) {
            throw new RuntimeException("FAIL: appending /async to " + base + " gave invalid URL", ex);
        }
    }
    
    /**
     * Generate a usable sync endpoint URL. This method only considers the specified
     * authentication method when performing the lookup of the base URL.
     * 
     * @param securityMethod IVOA securityMethod identifier
     * @return sync URL
     * @throws ResourceNotFoundException if base URL matching specified security method not found
     */
    public URL getSyncURL(URI securityMethod) 
            throws ResourceNotFoundException {
        Interface i = tap.findInterface(securityMethod);
        if (i == null) {
            throw new ResourceNotFoundException("not found: " + securityMethod + " in " + resourceID + "::" + tap.getStandardID());
        }
        String base = i.getAccessURL().getURL().toExternalForm();
        try {
            return new URL(base + "/sync");
        } catch (MalformedURLException ex) {
            throw new RuntimeException("FAIL: appending /sync to " + base + " gave invalid URL", ex);
        }
    }
    
    public Iterator<E> execute(String query, TapRowMapper<E> rowMapper) 
        throws AccessControlException, 
            ByteLimitExceededException, ExpectationFailedException, 
            IllegalArgumentException, PreconditionFailedException, 
            ResourceAlreadyExistsException, ResourceNotFoundException, 
            TransientException, IOException, InterruptedException {
        if (query == null) {
            throw new IllegalArgumentException("query: null");
        }
        if (rowMapper == null) {
            throw new IllegalArgumentException("rowMapper: null");
        }
        
        Subject s = AuthenticationUtil.getCurrentSubject();
        AuthMethod am = AuthenticationUtil.getAuthMethodFromCredentials(s);
        URI sm = Standards.getSecurityMethod(am);
        final URL syncURL = getSyncURL(sm);
        
        // execute MAXREC=0 query to get VOTable field metadata
        // and create formatter for each column
        Map<String,Object> params = new TreeMap<>();
        params.put("LANG", "ADQL");
        params.put("QUERY", query);
        params.put("RESPONSEFORMAT", VOTableWriter.CONTENT_TYPE);
        params.put("MAXREC", 0);
        log.debug("meta query: " + syncURL + " " + query);
        HttpPost meta = new HttpPost(syncURL, params, true);
        try {
            meta.prepare();
        } catch (IllegalArgumentException ex) {
            extractTapError(meta.getContentType(), ex);
        }
        
        if (!VOTableWriter.CONTENT_TYPE.equals(meta.getContentType())) {
            throw new RuntimeException("unexpected response: " + meta.getContentType() 
                    + " expected: " + VOTableWriter.CONTENT_TYPE);
        }
        
        VOTableReader r = new VOTableReader();
        VOTableDocument doc = r.read(meta.getInputStream());
        VOTableResource vr = doc.getResourceByType("results");
        VOTableTable vt = vr.getTable();
        FormatFactory formatFactory = new FormatFactory();
        List<Format> formatters = new ArrayList<>();
        for (VOTableField f : vt.getFields()) {
            Format fmt = formatFactory.getFormat(f);
            log.debug("field: " + f.getName() + " " + fmt.getClass().getName());
            formatters.add(fmt);
        }
        
        // execute full query with RESPONSEFORMAT=tsv so we can stream
        params.put("RESPONSEFORMAT", "tsv");
        params.remove("MAXREC");
        log.debug("stream query: " + syncURL + " " + query);
        HttpPost stream = new HttpPost(syncURL, params, true);
        stream.prepare();
        InputStream istream = stream.getInputStream();
        if (istream != null) {
            return new TsvIterator<E>(rowMapper, formatters, istream);
        }

        throw new RuntimeException("BUG: query response had InputStream: null");
    }
    
    // bad input -- IllegalArgumentException -- votable with error message
    private void extractTapError(String contentType, IllegalArgumentException ex)
            throws IllegalArgumentException {
        if (!VOTableWriter.CONTENT_TYPE.equals(contentType)) {
            throw ex;
        }
        try {
            VOTableReader r = new VOTableReader();
            VOTableDocument doc = r.read(new StringReader(ex.getMessage()));
            VOTableResource vr = doc.getResourceByType("results");
            for (VOTableInfo i : vr.getInfos()) {
                if (QUERY_STATUS.equals(i.getName()) && QUERY_STATUS_ERROR.equals(i.getValue())) {
                    throw new IllegalArgumentException(i.getValue() + ": " + i.content);
                }
            }
        } catch (IOException ex2) {
            throw new RuntimeException("failed to extract TAP error message", ex2);
        }
        throw ex;
    }
}
