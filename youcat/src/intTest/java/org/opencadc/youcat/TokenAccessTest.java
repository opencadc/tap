/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2021.                            (c) 2021.
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

package org.opencadc.youcat;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.AuthorizationToken;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.AuthChallenge;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.NetrcFile;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author majorb
 *
 */
public class TokenAccessTest {

    private static final Logger log = Logger.getLogger(TokenAccessTest.class);

    static Subject cadcAuthtest1Sub;

    static {
        Log4jInit.setLevel("ca.nrc.cadc.cat", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.net", Level.INFO);
        // need to read cert so we have creds to make the fake GMS call
        File cf = FileUtil.getFileFromResource("x509_CADCAuthtest1.pem", AuthQueryTest.class);
        cadcAuthtest1Sub = SSLUtil.createSubject(cf);
    }
    
    @Test
    public void useBearerTokenSuccess() {
        try {
            runTestSuccess(AuthenticationUtil.CHALLENGE_TYPE_BEARER);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void useBearerBadToken() {
        try {
            runTestAuthFail(AuthenticationUtil.CHALLENGE_TYPE_BEARER);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void useUnknownChallenge() {
        try {
            String token = getToken(cadcAuthtest1Sub);
            log.info("token: " + token);
            RegistryClient regClient = new RegistryClient();
            URL tablesURL = regClient.getServiceURL(Constants.RESOURCE_ID, Standards.VOSI_TABLES_11, AuthMethod.TOKEN);
            log.info("tables url: " + tablesURL);
            String domain = tablesURL.getHost();
            AuthorizationToken authToken = new AuthorizationToken("foo", token, Arrays.asList(domain));
            Subject s = new Subject();
            s.getPublicCredentials().add(authToken);
            Subject.doAs(s, new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                    HttpGet httpGet = new HttpGet(tablesURL, new ByteArrayOutputStream());
                    httpGet.run();
                    Assert.assertEquals(401, httpGet.getResponseCode());
                    Assert.assertFalse(httpGet.getResponseHeaderValues(AuthenticationUtil.AUTHENTICATE_HEADER).isEmpty());
                    Assert.assertNull(httpGet.getResponseHeader(AuthenticationUtil.VO_AUTHENTICATED_HEADER));
                    return null;
                }
            });
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void useWrongDomain() {
        try {
            String token = getToken(cadcAuthtest1Sub);
            log.info("token: " + token);
            RegistryClient regClient = new RegistryClient();
            URL tablesURL = regClient.getServiceURL(Constants.RESOURCE_ID, Standards.VOSI_TABLES_11, AuthMethod.TOKEN);
            log.info("tables url: " + tablesURL);
            String domain = "incorrect.domain.org";
            AuthorizationToken authToken = new AuthorizationToken("Bearer", token, Arrays.asList(domain));
            Subject s = new Subject();
            s.getPublicCredentials().add(authToken);
            Subject.doAs(s, new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                    HttpGet httpGet = new HttpGet(tablesURL, new ByteArrayOutputStream());
                    httpGet.run();
                    Assert.assertEquals(200, httpGet.getResponseCode());
                    Assert.assertFalse(httpGet.getResponseHeaderValues(AuthenticationUtil.AUTHENTICATE_HEADER).isEmpty());
                    Assert.assertNull(httpGet.getResponseHeader(AuthenticationUtil.VO_AUTHENTICATED_HEADER));
                    return null;
                }
            });
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    private String getToken(Subject s) throws PrivilegedActionException {
        return Subject.doAs(s, new PrivilegedExceptionAction<String>() {
            public String run() throws Exception {
                RegistryClient regClient = new RegistryClient();
                URL gmsURL = regClient.getServiceURL(Constants.GMS_RESOURSE_ID, Standards.SECURITY_METHOD_OAUTH, AuthMethod.CERT);
                log.info("gms url: " + gmsURL);
                URL authorizeURL = new URL(gmsURL.toString() + "?response_type=token");
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                HttpGet httpGet = new HttpGet(authorizeURL, out);
                httpGet.run();
                if (httpGet.getResponseCode() != 200) {
                    throw new Exception("Failed to get token: " + httpGet.getThrowable());
                }
                return out.toString();
            }
        });
    }
    
    private String doLogin() throws Exception {
        RegistryClient reg = new RegistryClient();
        URL capURL = reg.getServiceURL(Constants.RESOURCE_ID, Standards.VOSI_CAPABILITIES, AuthMethod.ANON);
        HttpGet head = new HttpGet(capURL, false);
        head.setHeadOnly(true);
        head.prepare();
        
        URL loginURL = null;
        List<String> authHeaders = head.getResponseHeaderValues("www-authenticate");
        // temp work-around for cadc-rest: it always injects an ivoa_x509 challenge
        List<String> modifiable = new ArrayList<>();
        modifiable.addAll(authHeaders);
        modifiable.remove("ivoa_x509");
        authHeaders = modifiable;
        
        for (String s : authHeaders) {
            log.info(s);
            AuthChallenge c = new AuthChallenge(s);
            log.info(c);
            if ("ivoa_bearer".equals(c.getName()) && Standards.SECURITY_METHOD_PASSWORD.toASCIIString().equals(c.getParamValue("standard_id"))) {
                loginURL = new URL(c.getParamValue("access_url"));
                break;
            }
        }
        
        if (loginURL == null) {
            throw new RuntimeException("no www-authenticate ivoa_bearer " + Standards.SECURITY_METHOD_PASSWORD.toASCIIString() + " challenge");
        }
        
        log.info("loginURL: " + loginURL);
        NetrcFile netrc = new NetrcFile();
        PasswordAuthentication up = netrc.getCredentials(loginURL.getHost(), true);
        if (up == null) {
            throw new RuntimeException("no credentials in .netrc file for host " + loginURL.getHost());
        } 
        
        Map<String,Object> params = new TreeMap<>();
        params.put("username", up.getUserName());
        params.put("password", up.getPassword());
        HttpPost login = new HttpPost(loginURL, params, true);
        login.prepare();
        String token = login.getResponseHeader("x-vo-bearer");
        Assert.assertNotNull("successful login", token);
        
        return token;
    }
    
    private void runTestSuccess(String challengeType) throws Exception {
        //String token = getToken(cadcAuthtest1Sub);
        String token = doLogin();
        log.info("token: " + token);
        RegistryClient regClient = new RegistryClient();
        
        URL tablesURL = regClient.getServiceURL(Constants.RESOURCE_ID, Standards.VOSI_TABLES_11, AuthMethod.TOKEN);
        log.info("tables url: " + tablesURL);
        String dom1 = tablesURL.getHost();
        
        URL gmsURL = regClient.getServiceURL(Constants.GMS_RESOURSE_ID, Standards.GMS_SEARCH_10, AuthMethod.TOKEN);
        String dom2 = gmsURL.getHost();
        
        AuthorizationToken authToken = new AuthorizationToken(challengeType, token, Arrays.asList(dom1, dom2));
        log.info("token: " + authToken);
        
        Subject s = new Subject();
        s.getPublicCredentials().add(authToken);
        Subject.doAs(s, new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                HttpGet httpGet = new HttpGet(tablesURL, new ByteArrayOutputStream());
                httpGet.run();
                Assert.assertEquals(200, httpGet.getResponseCode());
                String voAuthenticated = httpGet.getResponseHeader(AuthenticationUtil.VO_AUTHENTICATED_HEADER);
                Assert.assertNotNull(voAuthenticated);
                //Assert.assertEquals("cadcauthtest1", voAuthenticated);
                Assert.assertNull(httpGet.getResponseHeader(AuthenticationUtil.AUTHENTICATE_HEADER));
                return null;
            }
        });
    }
    
    private void runTestAuthFail(String challengeType) throws Exception {
        String token = "abcd";
        log.info("token: " + token);
        RegistryClient regClient = new RegistryClient();
        URL tablesURL = regClient.getServiceURL(Constants.RESOURCE_ID, Standards.VOSI_TABLES_11, AuthMethod.TOKEN);
        log.info("tables url: " + tablesURL);
        String domain = tablesURL.getHost();
        AuthorizationToken authToken = new AuthorizationToken(challengeType, token, Arrays.asList(domain));
        Subject s = new Subject();
        s.getPublicCredentials().add(authToken);
        Subject.doAs(s, new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                HttpGet httpGet = new HttpGet(tablesURL, new ByteArrayOutputStream());
                httpGet.run();
                Assert.assertEquals(401, httpGet.getResponseCode());
                Assert.assertFalse(httpGet.getResponseHeaderValues(AuthenticationUtil.AUTHENTICATE_HEADER).isEmpty());
                Assert.assertNull(httpGet.getResponseHeader(AuthenticationUtil.VO_AUTHENTICATED_HEADER));
                List<String> wwwAuthValues = httpGet.getResponseHeaderValues(AuthenticationUtil.AUTHENTICATE_HEADER);
                boolean errorMsgFound = false;
                boolean ivoaBearerPasswordFound = false;
                boolean ivoaBearerOpenIDFound = false;
                boolean ivoaX509PasswordFound = false;
                boolean bearerFound = false;
                for (String authValue : wwwAuthValues) {
                    String authValueLower = authValue.toLowerCase();
                    log.info("auth header: " + authValueLower);
                    if (authValueLower.startsWith(AuthenticationUtil.CHALLENGE_TYPE_IVOA_BEARER.toLowerCase())) {
                        if (authValueLower.contains(Standards.SECURITY_METHOD_PASSWORD.toString().toLowerCase())) {
                            ivoaBearerPasswordFound = true;
                        }
                        if (authValueLower.contains(Standards.SECURITY_METHOD_OPENID.toString().toLowerCase())) {
                            ivoaBearerOpenIDFound = true;
                        }
                        if (challengeType.toLowerCase().equals(AuthenticationUtil.CHALLENGE_TYPE_IVOA_BEARER.toLowerCase())) {
                            if (authValueLower.contains("error=\"") && authValueLower.contains("error_description=\"")) {
                                errorMsgFound = true;
                            }
                        }
                    }
                    if (authValueLower.startsWith(AuthenticationUtil.CHALLENGE_TYPE_IVOA_X509.toLowerCase())) {
                        if (authValueLower.contains(Standards.SECURITY_METHOD_HTTP_BASIC.toString().toLowerCase())) {
                            ivoaX509PasswordFound = true;
                        }
                    }
                    if (authValueLower.startsWith(AuthenticationUtil.CHALLENGE_TYPE_BEARER.toLowerCase())) {
                        bearerFound = true;
                        if (challengeType.toLowerCase().equals(AuthenticationUtil.CHALLENGE_TYPE_BEARER.toLowerCase())) {
                            if (authValueLower.contains("error=\"") && authValueLower.contains("error_description=\"")) {
                                errorMsgFound = true;
                            }
                        }
                    }
                }
                Assert.assertTrue(errorMsgFound);
                Assert.assertTrue(ivoaBearerPasswordFound);
                Assert.assertTrue(ivoaBearerOpenIDFound);
                Assert.assertTrue(ivoaX509PasswordFound);
                Assert.assertTrue(bearerFound);
                return null;
            }
        });
    }

    
}
