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

package org.opencadc.youcat.tap;

import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.DefaultTableWriter;
import ca.nrc.cadc.tap.MaxRecValidator;
import ca.nrc.cadc.tap.parser.extractor.SelectListExtractor;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.uws.ParameterUtil;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class MaxRecValidatorImpl extends MaxRecValidator {

    private static Logger log = Logger.getLogger(MaxRecValidatorImpl.class);

    // HACK: assume UTF-8 and mainly short strings
    // or numbers with ~8 decimal places
    private static final double BYTES_PER_COLUMN = 26.0;

    private static final double XML_BLOAT = 2.4;

    private static final long MAX_FILE_SIZE = 128 * 1024 * 1024; // 128 MB

    public MaxRecValidatorImpl() {
        super();
    }

    @Override
    public Integer validate() {
        if (sync) {
            // user specified limit only
            Integer maxrec = super.validate();
            log.debug("sync: maxrec=" + maxrec);
            return maxrec;
        }

        // async -> vospace: no enforced limit
        try {
            String destinationValue = ParameterUtil.findParameterValue("DEST", job.getParameterList());
            if (StringUtil.hasText(destinationValue)
                    && destinationValue.startsWith("vos://")) {
                Integer maxrec = super.validate();
                log.debug("async-vospace: maxrec=" + maxrec);
                return maxrec;
            }

            AdqlQueryHack adql = new AdqlQueryHack();
            adql.setJob(job);
            adql.setTapSchema(tapSchema);
            int numCols = adql.getSelectListSize();
            double rowSize = numCols * BYTES_PER_COLUMN;

            String format = ParameterUtil.findParameterValue("FORMAT", job.getParameterList());
            if (DefaultTableWriter.VOTABLE.equals(format)) {
                rowSize *= XML_BLOAT;
            }
            int numRows = (int) (MAX_FILE_SIZE / rowSize);
            setDefaultValue(numRows);
            setMaxValue(numRows);
            Integer maxrec = super.validate();
            log.debug("numCols=" + numCols + " rowSize=" + rowSize + " numRows=" + numRows + " dynamic async maxrec=" + maxrec);
            return maxrec;
        } finally {
            setDefaultValue(null);
            setMaxValue(null);
        }
    }

    private class AdqlQueryHack extends AdqlQuery {

        AdqlQueryHack() {
            super();
        }

        @Override
        protected void init() {
            super.init();
            if (tapSchema == null) // unit test mode
            {
                SelectNavigator keep = null;
                for (SelectNavigator sn : super.navigatorList) {
                    if (sn instanceof SelectListExtractor) {
                        keep = sn;
                    }
                }
                super.navigatorList.clear();
                super.navigatorList.add(keep);
            }
        }

        int getSelectListSize() {
            List items = super.getSelectList();
            return items.size();
        }
    }

}
