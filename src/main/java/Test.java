import utils.StreamSetsConnection;

import java.io.IOException;
import java.util.HashMap;


public class Test {

    public static void main(String[] args) throws IOException {
        StreamSetsConnection connection = new StreamSetsConnection();
        HashMap<String, String> response = connection.getPipelines();

        String id = response.get("test");
        System.out.println(connection.getMetrics(id));
    }
}
