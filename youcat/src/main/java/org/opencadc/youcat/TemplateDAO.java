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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.youcat;

import ca.nrc.cadc.tap.schema.AbstractDAO;
import java.util.List;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.opencadc.datalink.ServiceDescriptorTemplate;

/**
 * DAO for the ServiceDescriptor table.
 */
public class TemplateDAO extends AbstractDAO {

    /**
     * Create a TemplateDAO with a TapSchemaDAO, to use the same DataSource.
     *
     * @param abstractDAO a TapSchemaDAO
     */
    public TemplateDAO(AbstractDAO abstractDAO) {}

    /**
     * Create a TemplateDAO using a DataSource. Useful
     * for integration testing.
     *
     * @param dataSource a datasource
     */
    TemplateDAO(DataSource dataSource) {}

    /**
     * Get the template with the given name.
     *
     * @param owner the subject of the user getting the template
     * @param name  the template name
     * @return  a ServiceDescriptorTemplate or null if not found
     * @throws org.springframework.dao.DataAccessException if there is a problem querying the database.
     */
    public ServiceDescriptorTemplate get(Subject owner, String name) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Insert or update a template in the database.
     *
     * @param template the template to insert or update
     * @throws org.springframework.dao.DataAccessException if there is a problem inserting into the database.
     */
    public void put(ServiceDescriptorTemplate template) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Delete the template with the given name.
     *
     * @param owner the subject of the user deleting the template
     * @param name the name of the template to delete
     * @throws org.springframework.dao.DataAccessException if there is a problem deleting from the database.
     */
    public void delete(Subject owner, String name) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Get a List of templates that contain the given identifiers. The identifiers
     * are column_id's in the tap_schema.columns11 table.
     *
     * <p>Use case: injecting templates into TAP query results</p>
     *
     * @param identifiers the list of identifiers
     * @return a list of ServiceDescriptorTemplate's
     * @throws org.springframework.dao.DataAccessException if there is a problem querying the database.
     */
    public List<ServiceDescriptorTemplate> list(List<String> identifiers) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Get the list of templates owned by the given owner.
     *
     * <p>User case: listing templates owned by the caller in REST endpoint</p>
     *
     * @param owner the subject of the user
     * @return a list of ServiceDescriptorTemplate's
     */
    public List<ServiceDescriptorTemplate> list(Subject owner) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
