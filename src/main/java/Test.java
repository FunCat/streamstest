import com.streamsets.pipeline.sdk.ProcessorRunner;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;

public class Test {

    HttpClient httpclient = HttpClients.createDefault();
    HttpPost httppost = new HttpPost("http://www.a-domain.com/foo/");



}
