package pit.etl.updater;

import org.junit.*;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

import static pit.etl.updater.UpdaterMain.CONFIG;

/**
 * @since 4/5/2019
 */
public class UpdaterMainTest {
    String[] args = {"fbcs", "CCNNC_H190325105439"};
    String[] etl_batch_ids = Arrays.copyOfRange(args, 1, args.length);
    String sourceSystem = "HSFA";
    List<String> tableList = CONFIG.getStringList(args[0]);

    private static Connection connection;

    @BeforeClass
    public static void init() throws SQLException {

        connection = DriverManager.getConnection(CONFIG.get("db.url"));
        System.out.println(CONFIG.get("db.url"));
    }

    @Before
    public void setup() throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (String table : tableList) {
            sb.append("update ").append(table).append(" set source_system = ? where etl_batch_id in (");
            String comma = "";
            for (String s : etl_batch_ids) {
                sb.append(comma).append("?");
                comma = ", ";
            }
            sb.append(")\n");
        }
        try (PreparedStatement statement = connection.prepareStatement(sb.toString())) {
            int index = 1;
            for (String table : tableList) {
                statement.setObject(index++, sourceSystem);
                for (String anEtl_batch_idList : etl_batch_ids) {
                    statement.setObject(index++, anEtl_batch_idList);
                }
            }
            statement.executeUpdate();
        }
    }

    @Ignore("DJH 11/18/23 FIXME? ")
    @Test
    public void main() throws InterruptedException {
        UpdaterMain.main(args);

        for (String table : tableList) {
            StringBuilder sb = new StringBuilder("select count(*) from " + table + " where source_system <> 'CCNNC' and etl_batch_id in (");
            String comma = "";
            for (String s : etl_batch_ids) {
                sb.append(comma).append("?");
                comma = ", ";
            }
            sb.append(")");
            try (PreparedStatement statement = connection.prepareStatement(sb.toString())) {
                for (int i = 0; i < etl_batch_ids.length; i++) {
                    String etl_batch_id = etl_batch_ids[i];
                    statement.setObject(i + 1, etl_batch_id);
                }
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Assert.assertEquals(resultSet.getInt(1), 0);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @AfterClass
    public static void tireDown() {
//        try (PreparedStatement statement = connection.prepareStatement("delete")) {
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
    }
}