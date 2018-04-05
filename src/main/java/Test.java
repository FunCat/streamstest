import utils.StreamSetsConnection;

import java.io.IOException;
import java.util.HashMap;


public class Test {

    public static void main(String[] args) throws IOException, InterruptedException {
        StreamSetsConnection connection = new StreamSetsConnection();
        HashMap<String, String> pipelines = connection.getPipelines();

        String id = pipelines.get("test");
        connection.startPipelines(pipelines);
        while(true) {

            String m5Rate = connection.getMetrics(id);
            float m5RateFloat = Float.parseFloat(m5Rate);
            if(m5RateFloat > 0.9f){
                connection.stopPipeline(id);
                break;
            }
            System.out.println(m5Rate);
            Thread.sleep(1000);
        }
    }
}
