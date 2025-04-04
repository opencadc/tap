/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2019.                            (c) 2019.
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

package ca.nrc.cadc.tap.schema;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.cred.client.CredUtil;
import java.security.Principal;
import java.util.Set;
import java.util.TreeSet;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.gms.IvoaGroupClient;

/**
 * Class that checks authorization on TapPermissions.
 * 
 * @author majorb
 *
 */
public class TapAuthorizer {
    
    protected static Logger log = Logger.getLogger(TapAuthorizer.class);
    
    public TapAuthorizer() {
    }
    
    /**
     * Check if the current user has read permission according to the given
     * TapPermissions.
     * 
     * @param tp The permissions to check.
     */
    public boolean hasReadPermission(TapPermissions tp) {

        // first check if the table is public
        if (tp == null) {
            log.debug("public: no tap permissions");
            return true;
        }
        if (tp.owner == null) {
            log.debug("public: no owner in tap permissions");
            return true;
        }
        if (tp.owner != null && tp.isPublic != null && tp.isPublic) {
            log.debug("public: set as public");
            return true;
        }

        Subject curSub = AuthenticationUtil.getCurrentSubject();
        boolean anon = curSub == null || curSub.getPrincipals().isEmpty();

        if (!anon) {
            if (isOwner(tp.owner, curSub)) {
                log.debug("caller is owner");
                return true;
            }
            try {
                if (tp.readGroup != null || tp.readWriteGroup != null) {
                    return isMember(tp.readGroup, tp.readWriteGroup);
                }
            } catch (Exception e) {
                log.error("error getting groups or checking credentials", e);
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    // return true if the two subjects have a common principal
    private boolean isOwner(Subject owner, Subject caller) {
        Set<Principal> ownerPrincipals = owner.getPrincipals();
        Set<Principal> callerPrincipals = caller.getPrincipals();
        for (Principal op : ownerPrincipals) {
            for (Principal cp : callerPrincipals) {
                if (AuthenticationUtil.equals(op, cp)) {
                    return true;
                }
            }
        }
        return false;
    }

    // check membership
    private boolean isMember(GroupURI g1, GroupURI g2) throws Exception {
        IvoaGroupClient groupClient = new IvoaGroupClient();
        boolean hasCDPCreds = false;
        try {
            hasCDPCreds = CredUtil.checkCredentials();
        } catch (Exception e) {
            log.debug("Failed to check for valid CDP credentials needed for group membership check");
            throw e;
        }
        if (hasCDPCreds) {
            Set<GroupURI> groups = new TreeSet();
            if (g1 != null) {
                groups.add(g1);
            }
            if (g2 != null) {
                groups.add(g2);
            }
            Set<GroupURI> membership = groupClient.getMemberships(groups);
            return !membership.isEmpty();
        } else {
            log.debug("No CDP credentials available to allow group membership check");
        }
        return false;
    }

}
