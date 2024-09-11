package com.github.dwladdimiroc.serverlessApp.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class Serverless {
    private static final Logger logger = LoggerFactory.getLogger(Serverless.class);
    private static final String GCP_URI = "https://europe-west9-sps-storm.cloudfunctions.net/";
    private static final String USER_AGENT = "Mozilla/5.0";
    public static final int LIMIT_FUNCTIONS = 250;
    public static final double USE_SERVERLESS = 0.2;

    public static HttpRequest request(String name, int numReplica, int size) {
        int idFunction = numReplica % LIMIT_FUNCTIONS;
        String uri = GCP_URI + name + "-" + idFunction + "?number=" + size;
        return HttpRequest.newBuilder().uri(URI.create(uri)).GET().build();
    }

    public static void function(HttpClient client, HttpRequest request) {
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            //logger.info("url={},resp={}", request.uri(), response.body());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
