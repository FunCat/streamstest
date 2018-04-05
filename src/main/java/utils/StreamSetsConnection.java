package utils;

import _ss_com.fasterxml.jackson.databind.JsonNode;
import _ss_com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class StreamSetsConnection {

    private String ssdcUser = "admin";
    private String ssdcPassword = "admin";

    private String host = "http://localhost";
    private String port = "18630";

    private ObjectMapper objectMapper = new ObjectMapper();

    public StreamSetsConnection() {
    }

    public StreamSetsConnection(String ssdcUser, String ssdcPassword, String host, String port) {
        this.ssdcUser = ssdcUser;
        this.ssdcPassword = ssdcPassword;
        this.host = host;
        this.port = port;
    }

    private String responseToString(HttpResponse response) throws IOException {
        BufferedReader rd = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent()));

        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        return result.toString();
    }

    private HttpResponse getRequest(String url) throws IOException {
        String rightUrl = host + ":" + port + url;
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(rightUrl);
        request.addHeader(BasicScheme.authenticate(
                new UsernamePasswordCredentials(ssdcUser, ssdcPassword),
                "UTF-8", false));

        return client.execute(request);
    }

    private HttpResponse postRequest(String url) throws IOException {
        String rightUrl = host + ":" + port + url;
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost(rightUrl);
        request.addHeader(BasicScheme.authenticate(
                new UsernamePasswordCredentials(ssdcUser, ssdcPassword),
                "UTF-8", false));
        request.addHeader("X-Requested-By", "sdc");

        return client.execute(request);
    }

    public HashMap<String, String> getPipelines() throws IOException {
        HashMap<String, String> pipelines = new HashMap<>();

        HttpResponse httpResponse = getRequest("/rest/v1/pipelines");
        String response = responseToString(httpResponse);

        JsonNode actualObj = objectMapper.readTree(response);

        if(actualObj.isArray()){
            for (JsonNode jsonNode : actualObj) {
                pipelines.put(jsonNode.get("title").asText(), jsonNode.get("pipelineId").asText());
            }
        }

        return pipelines;
    }

    public boolean startPipeline(String pipelineId) throws IOException, InterruptedException {
        HttpResponse httpResponse = postRequest("/rest/v1/pipeline/" + pipelineId + "/start?rev=0");

        if(httpResponse.getStatusLine().getStatusCode() == 200){
            while(!isRunning(pipelineId)){
                Thread.sleep(1000);
            }
            return true;
        }
        return false;
    }

    public boolean stopPipeline(String pipelineId) throws IOException {
        HttpResponse httpResponse = postRequest("/rest/v1/pipeline/" + pipelineId + "/stop?rev=0");

        if(httpResponse.getStatusLine().getStatusCode() == 200){
            return true;
        }
        return false;
    }

    public boolean forceStopPipeline(String pipelineId) throws IOException {
        HttpResponse httpResponse = postRequest("/rest/v1/pipeline/" + pipelineId + "/forceStop?rev=0");

        if(httpResponse.getStatusLine().getStatusCode() == 200){
            return true;
        }
        return false;
    }

    public boolean isRunning(String pipelineId) throws IOException {
        HttpResponse httpResponse = getRequest("/rest/v1/pipeline/" + pipelineId + "/status?rev=0");
        String response = responseToString(httpResponse);

        JsonNode actualObj = objectMapper.readTree(response);
        String status = actualObj.get("status").asText();

        if("RUNNING".equals(status)){
            return true;
        }
        return false;
    }

    public String getMetrics(String pipelineId) throws IOException {
        if(isRunning(pipelineId)) {
            HttpResponse httpResponse = getRequest("/rest/v1/pipeline/" + pipelineId + "/metrics?rev=0");
            String response = responseToString(httpResponse);
            JsonNode actualObj = objectMapper.readTree(response);
            return actualObj.get("timers").get("pipeline.batchProcessing.timer").get("m5_rate").asText();
        }

        return "Pipeline isn't running!";
    }
}
