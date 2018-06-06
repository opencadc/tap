/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2014.                         (c) 2014.
 * National Research Council            Conseil national de recherches
 * Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 * All rights reserved                  Tous droits reserves
 *
 * NRC disclaims any warranties         Le CNRC denie toute garantie
 * expressed, implied, or statu-        enoncee, implicite ou legale,
 * tory, of any kind with respect       de quelque nature que se soit,
 * to the software, including           concernant le logiciel, y com-
 * without limitation any war-          pris sans restriction toute
 * ranty of merchantability or          garantie de valeur marchande
 * fitness for a particular pur-        ou de pertinence pour un usage
 * pose.  NRC shall not be liable       particulier.  Le CNRC ne
 * in any event for any damages,        pourra en aucun cas etre tenu
 * whether direct or indirect,          responsable de tout dommage,
 * special or general, consequen-       direct ou indirect, particul-
 * tial or incidental, arising          ier ou general, accessoire ou
 * from the use of the software.        fortuit, resultant de l'utili-
 *                                      sation du logiciel.
 *
 *
 * @author jenkinsd
 * 16/07/14 - 12:13 PM
 *
 *
 *
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 ************************************************************************
 */

package ca.nrc.cadc.tap.upload.datatype;

import java.sql.Types;

import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TapDataType;


public class OracleDataType extends BasicDataTypeMapper {
    // HACK: arbitrary sensible limit.  Maximum is 4000 for Oracle.
    private static final int DEFAULT_VARCHAR2_QUANTIFIER = 3072;


    public OracleDataType() {
        dataTypes.put(TapDataType.INTEGER, new TypePair("INT", Types.INTEGER));
        dataTypes.put(TapDataType.CLOB, new TypePair("VARCHAR2", Types.VARCHAR));
    }


    @Override
    public String getDataType(final ColumnDesc columnDesc) {
        final StringBuilder columnDataType = new StringBuilder();
        final TapDataType tapDataType = columnDesc.getDatatype();
        columnDataType.append(dataTypes.get(tapDataType).str);

        if (tapDataType.isVarSize()) {
            columnDataType.append("(");
            columnDataType.append(DEFAULT_VARCHAR2_QUANTIFIER);
            columnDataType.append(")");
        } else if (isInteger(tapDataType.arraysize)) {
            columnDataType.append("(");
            columnDataType.append(parseInteger(tapDataType.arraysize));
            columnDataType.append(")");
        }

        return columnDataType.toString();
    }

    private boolean isInteger(final String input) {
        try {
            Integer.parseInt(input);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private int parseInteger(final String input) {
        try {
            return Integer.parseInt(input);
        } catch (NumberFormatException e) {
            return new Double(Double.NaN).intValue();
        }
    }
}
