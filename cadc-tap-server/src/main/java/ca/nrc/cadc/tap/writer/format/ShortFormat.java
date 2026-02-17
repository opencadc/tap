package ca.nrc.cadc.tap.writer.format;

import ca.nrc.cadc.db.mappers.JdbcMapUtil;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ShortFormat extends AbstractResultSetFormat {

    private static final ca.nrc.cadc.dali.util.ShortFormat fmt = new ca.nrc.cadc.dali.util.ShortFormat();

    @Override
    public Object extract(ResultSet resultSet, int columnIndex) throws SQLException {
        Short val = JdbcMapUtil.getShort(resultSet, columnIndex);
        return val;
    }

    @Override
    public String format(Object o) {
        return fmt.format((short) o);
    }
}
