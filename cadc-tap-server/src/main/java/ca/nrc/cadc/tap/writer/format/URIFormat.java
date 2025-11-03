package ca.nrc.cadc.tap.writer.format;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;

public class URIFormat extends AbstractResultSetFormat {

    private static final ca.nrc.cadc.dali.util.URIFormat fmt = new ca.nrc.cadc.dali.util.URIFormat();

    @Override
    public Object extract(ResultSet resultSet, int columnIndex) throws SQLException {
        Object object = resultSet.getObject(columnIndex);
        if (object == null) {
            return null;
        }
        if (object instanceof String) {
            object = URI.create((String) object);
        }
        if (object instanceof URI) {
            return object;
        }
        throw new IllegalArgumentException(object.getClass().getName() + " not supported");
    }

    @Override
    public String format(Object o) {
        return fmt.format((URI) o);
    }
}
