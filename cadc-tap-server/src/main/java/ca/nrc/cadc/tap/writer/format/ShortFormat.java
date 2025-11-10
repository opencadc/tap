package ca.nrc.cadc.tap.writer.format;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ShortFormat extends AbstractResultSetFormat {

    private static final ca.nrc.cadc.dali.util.ShortFormat fmt = new ca.nrc.cadc.dali.util.ShortFormat();

    @Override
    public Object extract(ResultSet resultSet, int columnIndex) throws SQLException {
        Object object = resultSet.getObject(columnIndex);
        if (object == null) {
            return null;
        }
        if (object instanceof Integer) {
            int intValue = (int) object;
            if (intValue < Short.MIN_VALUE || intValue > Short.MAX_VALUE) {
                throw new IllegalArgumentException("Value out of range for Short: " + intValue);
            }
            object = (short) intValue;
        }
        if (object instanceof Short) {
            return object;
        }
        throw new IllegalArgumentException("Short Type expected. Found : " + object.getClass().getName());
    }

    @Override
    public String format(Object o) {
        return fmt.format((short) o);
    }
}
