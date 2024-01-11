package pit.util;

import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.stream.Collectors;

public class JdbcUtils {

    public static Timestamp currentTimestamp() {
        return new Timestamp(System.currentTimeMillis());
    }

    public static String placeholdersFromColumns(String columnsStr) {

        String paceholdersStr = StringUtils.repeat("?,", StringUtils.split(columnsStr, ',').length);
        // get rid of last ,
        paceholdersStr = paceholdersStr.substring(0, paceholdersStr.length() - 1);

        return paceholdersStr;

    }

    public static String genListForIn(Collection<String> vals) {
        return vals.stream().map(JdbcUtils::wrap).collect( Collectors.joining( "," ) );
    }

    public static String boolToYesNo(boolean bool) {
        return bool ? "Y" : "N";
    }

    public static boolean YNToBool(String yn) {
        return yn != null && StringUtils.equalsIgnoreCase(yn, "Y");
    }

    public static String getUserName() {
        return System.getProperty("user.name");
    }

    public static String wrap(String val) {
        return StringUtils.wrap(val, '\'');
    }

    @SuppressWarnings("unused")
    public static String likeSubstr(String val) {
        return " like '%" + val + "%'";
    }

    public static String blankToNull(String val) {
        if (StringUtils.isBlank(val)) {
            val=null;
        }
        return null;
    }

}