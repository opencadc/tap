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
 ************************************************************************
 */

package ca.nrc.cadc.tap.writer.format;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Oracle {@code uuid} xtype: values are stored as {@code VARCHAR2} and exchanged as DALI {@link UUID}.
 */
public class OracleUUIDFormat extends AbstractResultSetFormat {

    private final ca.nrc.cadc.dali.util.UUIDFormat fmt = new ca.nrc.cadc.dali.util.UUIDFormat();

    @Override
    public Object parse(String s) {
        return fmt.parse(s);
    }

    @Override
    public Object extract(ResultSet resultSet, int columnIndex) throws SQLException {
        String s = resultSet.getString(columnIndex);
        if (s == null) {
            return null;
        }
        return fmt.parse(s.trim());
    }

    @Override
    public String format(Object o) {
        return fmt.format((UUID) o);
    }
}
