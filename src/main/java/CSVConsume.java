import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class CSVConsume {
    public static void main(String[] args){
        String jdbcUrl = "jdbc:mysql://localhost:3306/batch";
        String userName = "root";
        String password = "1234";

        String filePath = "src/main/resources/records.csv";

        int batchSize = 20;

        Connection connection = null;

        long rows = 20;

        try{
            connection = DriverManager.getConnection(jdbcUrl, userName, password);
            connection.setAutoCommit(false);

            String sql = "insert into PinCodeData(officeName, pincode, district, state) values(?,?,?,?)";

            PreparedStatement statement = connection.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(filePath));

            String lineText = null;
            int count = 0;

            lineReader.readLine();
            while((lineText=lineReader.readLine())!=null){
                String[] data = lineText.split(",");

                String officeName = data[3];
                String pincode = data[4];
                String district = data[7];
                String state = data[8];

                statement.setString(1,officeName);
                statement.setString(2,pincode);
                statement.setString(3,district);
                statement.setString(4,state);
                statement.addBatch();
                if(count%batchSize == 0){
                    statement.executeBatch();
                    System.out.println(rows + "has been inserted");
                    rows+=20;
                }
            }

            lineReader.close();
            statement.executeBatch();
            connection.commit();
            connection.close();
            System.out.println("data has been inserted successfully");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
