package com.github.dwladdimiroc.serverlessApp.util;

import java.net.HttpURLConnection;
import java.net.URL;

public class HTTP {
    private static final String USER_AGENT = "Mozilla/5.0";

    public static void function(int size) {
        try {
            String url = "https://europe-west1-sps-storm.cloudfunctions.net/BoltProcess?number=" + size;

            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("User-Agent", USER_AGENT);
            con.getResponseCode();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
