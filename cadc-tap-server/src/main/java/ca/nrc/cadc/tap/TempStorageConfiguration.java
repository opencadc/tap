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
 *
 ************************************************************************
 */

package ca.nrc.cadc.tap;

import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import org.apache.log4j.Logger;

import java.io.File;


/**
 * Configuration for the temporary storage in TAP for results and upload storage.
 */
public class TempStorageConfiguration {
    private static final Logger LOGGER = Logger.getLogger(TempStorageConfiguration.class);

    private static final String CONFIG_KEY = "org.opencadc.tap";

    private static final String DEFAULT_CONFIG_FILE = CONFIG_KEY + ".properties";
    private static final String BASE_DIR_KEY = CONFIG_KEY + ".baseStorageDir";
    private static final String BASE_URL_KEY = CONFIG_KEY + ".baseURL";

    public static final String DOWNLOAD_ENDPOINT = "/files";

    private final File baseDir;
    private String baseURL;


    /**
     * Constructore for configuration.
     * @param baseDir   Required base directory.
     * @param baseURL   Optional base URL.  Will assume current request URL.
     */
    TempStorageConfiguration(final File baseDir, final String baseURL) {
        this.baseDir = baseDir;
        this.baseURL = baseURL;
    }

    public File getBaseDir() {
        return baseDir;
    }

    public String getBaseURL() {
        return baseURL;
    }

    public void setBaseURL(String baseURL) {
        this.baseURL = baseURL;
    }

    /**
     * Load a new configuration with the default configuration file location.
     *
     * @return  TempStorageConfiguration instance.  Never null.
     */
    public static TempStorageConfiguration load() {
        return TempStorageConfiguration.load(DEFAULT_CONFIG_FILE);
    }

    /**
     * Load the given configuration file to populate this configuration.
     * @param configurationFileName     The configuration file located in ${user.home}/config or in the
     *                                  ${env.DEFAULT_CONFIG_DIR} folder.
     * @return  A new TempStorageConfiguration instance.  Never null.
     */
    public static TempStorageConfiguration load(final String configurationFileName) {
        final PropertiesReader r = new PropertiesReader(configurationFileName);
        final MultiValuedProperties props = r.getAllProperties();

        if (LOGGER.isDebugEnabled()) {
            for (final String s : props.keySet()) {
                LOGGER.debug("props: " + s + "=" + props.getProperty(s));
            }
        }

        final String baseURL;

        if (!props.keySet().contains(BASE_DIR_KEY)) {
            final String message = "CONFIG: incomplete: " + BASE_DIR_KEY + " is required in the <init-param> section.";
            LOGGER.error(message);
            throw new RuntimeException(message);
        } else if (!props.keySet().contains(BASE_URL_KEY)) {
            LOGGER.warn(BASE_URL_KEY + " is missing.  The request URL will be used instead at runtime.");
            baseURL = null;
        } else {
            // TODO: /files has to match servlet-mapping for this in web.xml
            baseURL = props.getFirstPropertyValue(BASE_URL_KEY) + TempStorageConfiguration.DOWNLOAD_ENDPOINT;
        }

        final File baseDir = new File(props.getFirstPropertyValue(BASE_DIR_KEY));

        if (!baseDir.exists()) {
            final boolean successful = baseDir.mkdirs();
            LOGGER.debug(baseDir.getPath() + " successfully created?: " + (successful ? "OK" : "FAILED"));
        }
        if (!baseDir.exists()) {
            throw new RuntimeException(BASE_DIR_KEY + "=" + baseDir + " does not exist, cannot create");
        }
        if (!baseDir.isDirectory()) {
            throw new RuntimeException(BASE_DIR_KEY + "=" + baseDir + " is not a directory");
        }
        if (!baseDir.canRead() || !baseDir.canWrite()) {
            throw new RuntimeException(BASE_DIR_KEY + "=" + baseDir + " is not readable && writable");
        }

        return new TempStorageConfiguration(baseDir, baseURL);
    }
}
