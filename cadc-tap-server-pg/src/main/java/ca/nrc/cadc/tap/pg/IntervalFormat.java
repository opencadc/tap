/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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

package ca.nrc.cadc.tap.pg;

import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.postgresql.PgInterval;
import ca.nrc.cadc.dali.util.DoubleIntervalArrayFormat;
import ca.nrc.cadc.dali.util.DoubleIntervalFormat;
import ca.nrc.cadc.tap.writer.format.AbstractResultSetFormat;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class IntervalFormat extends AbstractResultSetFormat {
    private static final Logger log = Logger.getLogger(IntervalFormat.class);

    private DoubleIntervalFormat fmt = new DoubleIntervalFormat();
    private DoubleIntervalArrayFormat afmt = new DoubleIntervalArrayFormat();
    private boolean intervalArray;

    public IntervalFormat(boolean intervalArray) {
        this.intervalArray = intervalArray;
    }

    @Override
    public Object extract(ResultSet resultSet, int columnIndex)
            throws SQLException {
        String s = resultSet.getString(columnIndex);
        if (s == null) {
            return null;
        }

        PgInterval pgi = new PgInterval();
        if (intervalArray) {
            return pgi.getIntervalArray(s);
        }
        return pgi.getInterval(s);
    }

    @Override
    public String format(Object object) {
        if (object == null) {
            return "";
        }
        
        if (object instanceof Interval[]) {
            Interval<Double>[] di = (Interval<Double>[]) object;
            return afmt.format(di);
        }
        if (object instanceof Interval) {
            Interval<Double> di = (Interval<Double>) object;
            return fmt.format(di);
        }
        
        // this might help debugging more than a throw
        return object.toString();
    }
}
