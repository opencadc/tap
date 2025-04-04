/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2009.                            (c) 2009.
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

package ca.nrc.cadc.tap.writer.format;

import ca.nrc.cadc.dali.util.DefaultFormat;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.tap.TapSelectItem;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.uws.Job;
import java.util.ArrayList;
import java.util.List;

/**
 * Returns a Formatter for a given data type.
 *
 */
public class DefaultFormatFactory implements FormatFactory {

    protected Job job;

    public DefaultFormatFactory() {
    }

    public void setJob(Job job) {
        this.job = job;
    }

    @Override
    public List<Format<Object>> getFormats(List<TapSelectItem> selectList) {
        List<Format<Object>> formats = new ArrayList<Format<Object>>();
        for (TapSelectItem paramDesc : selectList) {
            if (paramDesc != null) {
                formats.add(getFormat(paramDesc));
            }
        }
        return formats;
    }

    /**
     * Return the default format when no type-specific one is found.
     *
     * @return a DefaultFormat
     */
    protected Format<Object> getDefaultFormat() {
        return new DefaultFormat();
    }

    /**
     * Create a formatter for the specified parameter description. The default implementation simply
     * checks the datatype in the argument and then calls the appropriate (public) get.type.Formatter
     * method. Subclasses should override this method if they need to support additional datatypes
     * (as specified in the TapSchema: tap_schema.columns.datatype).
     *
     * @param item
     * @return
     */
    @Override
    public Format<Object> getFormat(TapSelectItem item) {
        TapDataType tt = item.getDatatype();
        String datatype = tt.getDatatype();

        if (tt.xtype != null) {
            if (datatype.equals("char") && "timestamp".equals(tt.xtype)) {
                return new UTCTimestampFormat(); // DALI-1.1
            }

            if ("point".equals(tt.xtype)) {
                return getPointFormat(item); // DALI-1.1
            }

            if ("circle".equals(tt.xtype)) {
                return getCircleFormat(item); // DALI-1.1
            }

            if ("polygon".equals(tt.xtype)) {
                return getPolygonFormat(item); // DALI-1.1
            }

            if ("interval".equals(tt.xtype)) {
                return getIntervalFormat(item); // DALI-1.1
            }

            if (tt.xtype.endsWith("multiinterval")) {
                return getMultiIntervalFormat(item); // proposed DALI-1.2, ignore prefix
            }

            if (tt.xtype.endsWith("multipolygon")) {
                return getMultiPolygonFormat(item);// proposed DALI-1.2, ignore prefix
            }

            if (tt.xtype.endsWith("shape")) {
                return getShapeFormat(item); // proposed DALI-1.2, ignore prefix
            }

            //if (tt.xtype.endsWith("region"))
            //{
            //    return getRegionFormat(item); // proposed DALI-1.2, ignore prefix??
            //}
            if ("uuid".equals(tt.xtype)) {
                return getUUIDFormat(item);// proposed DALI-1.2
            }

            if ("uri".equals(tt.xtype)) {
                return getStringFormat(item);// proposed DALI-1.2
            }

            if ("clob".equals(tt.xtype)) {
                return getClobFormat(item); // custom or ADQL-2.1?
            }

            // unsupported: boolean, bit, floatComplex, doubleComplex
            // TAP-1.0 ADQL types for backwards compatibility
            if ("adql:POINT".equalsIgnoreCase(tt.xtype)) {
                return getPositionFormat(item);
            }

            if ("adql:REGION".equalsIgnoreCase(tt.xtype)) {
                return getRegionFormat(item);
            }

            if ("adql:TIMESTAMP".equalsIgnoreCase(tt.xtype)) {
                return new UTCTimestampFormat();
            }

            if ("adql:CLOB".equalsIgnoreCase(tt.xtype)) {
                return getClobFormat(item);
            }

            if ("adql:BLOB".equalsIgnoreCase(tt.xtype)) {
                return getBlobFormat(item);
            }
        }

        // primitive types
        if (datatype.equalsIgnoreCase("char")) {
            return getStringFormat(item);
        }

        if (datatype.equalsIgnoreCase("unsignedByte")) {
            if (tt.arraysize != null) {
                return getByteArrayFormat(item);
            } else {
                return getByteFormat(item);
            }
        }

        if (datatype.equalsIgnoreCase("short")) {
            if (tt.arraysize != null) {
                return getShortArrayFormat(item);
            } else {
                return getShortFormat(item);
            }
        }

        if (datatype.equalsIgnoreCase("int")) {
            if (tt.arraysize != null) {
                return getIntArrayFormat(item);
            } else {
                return getIntegerFormat(item);
            }
        }

        if (datatype.equalsIgnoreCase("long")) {
            if (tt.arraysize != null) {
                return getLongArrayFormat(item);
            } else {
                return getLongFormat(item);
            }
        }

        if (datatype.equalsIgnoreCase("float")) {
            if (tt.arraysize != null) {
                return getFloatArrayFormat(item);
            } else {
                return getRealFormat(item);
            }
        }

        if (datatype.equalsIgnoreCase("double")) {
            if (tt.arraysize != null) {
                return getDoubleArrayFormat(item);
            } else {
                return getDoubleFormat(item);
            }
        }

        return getDefaultFormat();
    }

    /**
     * @param columnDesc
     * @return a DefaultFormat
     */
    protected Format<Object> getByteFormat(TapSelectItem columnDesc) {
        return getDefaultFormat();
    }

    /**
     * @param columnDesc
     * @return a DefaultFormat
     */
    protected Format<Object> getShortFormat(TapSelectItem columnDesc) {
        return getDefaultFormat();
    }

    /**
     * @param columnDesc
     * @return a DefaultFormat
     */
    protected Format<Object> getIntegerFormat(TapSelectItem columnDesc) {
        return getDefaultFormat();
    }

    /**
     * @param columnDesc
     * @return a DefaultFormat
     */
    protected Format<Object> getRealFormat(TapSelectItem columnDesc) {
        return getDefaultFormat();
    }

    /**
     * @param columnDesc
     * @return a DefaultFormat
     */
    protected Format<Object> getDoubleFormat(TapSelectItem columnDesc) {
        return getDefaultFormat();
    }

    /**
     * @param columnDesc
     * @return a DefaultFormat
     */
    protected Format<Object> getLongFormat(TapSelectItem columnDesc) {
        return getDefaultFormat();
    }

    /**
     * @param columnDesc
     * @return a DefaultFormat
     */
    protected Format<Object> getStringFormat(TapSelectItem columnDesc) {
        return getDefaultFormat();
    }

    /**
     * @param columnDesc
     * @return a ByteArrayFormat
     */
    protected Format<Object> getByteArrayFormat(TapSelectItem columnDesc) {
        return new ByteArrayFormat();
    }

    /**
     * @param columnDesc
     * @return an IntArrayFormat
     */
    protected Format<Object> getShortArrayFormat(TapSelectItem columnDesc) {
        return new ShortArrayFormat();
    }

    /**
     * @param columnDesc
     * @return an IntArrayFormat
     */
    protected Format<Object> getIntArrayFormat(TapSelectItem columnDesc) {
        return new IntArrayFormat();
    }

    /**
     * @param columnDesc
     * @return an LongArrayFormat
     */
    protected Format<Object> getLongArrayFormat(TapSelectItem columnDesc) {
        return new LongArrayFormat();
    }

    /**
     * @param columnDesc
     * @return an FloatArrayFormat
     */
    protected Format<Object> getFloatArrayFormat(TapSelectItem columnDesc) {
        return new FloatArrayFormat();
    }

    /**
     * @param columnDesc
     * @return an DoubleArrayFormat
     */
    protected Format<Object> getDoubleArrayFormat(TapSelectItem columnDesc) {
        return new DoubleArrayFormat();
    }

    /**
     * @param columnDesc
     * @return a UTCTimestampFormat
     */
    protected Format<Object> getTimestampFormat(TapSelectItem columnDesc) {
        return new UTCTimestampFormat();
    }

    /**
     * @param columnDesc
     * @throws UnsupportedOperationException
     */
    protected Format<Object> getPointFormat(TapSelectItem columnDesc) {
        throw new UnsupportedOperationException("no formatter for column " + columnDesc.getName());
    }

    /**
     * @param columnDesc
     * @throws UnsupportedOperationException
     */
    protected Format<Object> getCircleFormat(TapSelectItem columnDesc) {
        throw new UnsupportedOperationException("no formatter for column " + columnDesc.getName());
    }

    /**
     * @param columnDesc
     * @throws UnsupportedOperationException
     */
    protected Format<Object> getPolygonFormat(TapSelectItem columnDesc) {
        throw new UnsupportedOperationException("no formatter for column " + columnDesc.getName());
    }

    /**
     * @param columnDesc
     * @throws UnsupportedOperationException
     */
    protected Format<Object> getMultiPolygonFormat(TapSelectItem columnDesc) {
        throw new UnsupportedOperationException("no formatter for column " + columnDesc.getName());
    }

    protected Format<Object> getShapeFormat(TapSelectItem columnDesc) {
        throw new UnsupportedOperationException("no formatter for column " + columnDesc.getName());
    }

    /**
     * @param columnDesc
     * @throws UnsupportedOperationException
     */
    protected Format<Object> getPositionFormat(TapSelectItem columnDesc) {
        throw new UnsupportedOperationException("no formatter for column " + columnDesc.getName());
    }

    /**
     * @param columnDesc
     * @throws UnsupportedOperationException
     */
    protected Format<Object> getRegionFormat(TapSelectItem columnDesc) {
        throw new UnsupportedOperationException("no formatter for column " + columnDesc.getName());
    }

    /**
     * @param columnDesc
     * @throws UnsupportedOperationException
     */
    protected Format<Object> getIntervalFormat(TapSelectItem columnDesc) {
        throw new UnsupportedOperationException("no formatter for column " + columnDesc.getName());
    }

    /**
     * @param columnDesc
     * @throws UnsupportedOperationException
     */
    protected Format<Object> getMultiIntervalFormat(TapSelectItem columnDesc) {
        throw new UnsupportedOperationException("no formatter for column " + columnDesc.getName());
    }

    /**
     * @param columnDesc
     * @return a DefaultFormat
     * @throws UnsupportedOperationException
     */
    protected Format<Object> getBlobFormat(TapSelectItem columnDesc) {
        return new ByteArrayFormat();
    }

    /**
     * @param columnDesc
     * @return a DefaultFormat
     * @throws UnsupportedOperationException
     */
    protected Format<Object> getClobFormat(TapSelectItem columnDesc) {
        return getDefaultFormat();
    }

    /**
     * @param columnDesc
     * @return a DefaultFormat
     * @throws UnsupportedOperationException
     */
    protected Format<Object> getUUIDFormat(TapSelectItem columnDesc) {
        return getDefaultFormat();
    }
}
