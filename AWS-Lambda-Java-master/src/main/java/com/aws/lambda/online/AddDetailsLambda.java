package com.aws.lambda.online;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.aws.lambda.online.data.TVSeries;
import com.aws.lambda.online.data.ResponseDetails;

import org.json.*;

public class AddDetailsLambda implements RequestHandler<TVSeries, ResponseDetails> {

    private String tmdbKey = "";

    public ResponseDetails handleRequest(TVSeries requestDetails, Context arg1) {

        ResponseDetails responseDetails = new ResponseDetails();
        try {
            insertDetails(requestDetails, responseDetails);
        } catch (SQLException sqlException) {
            responseDetails.setMessageID("999");
            responseDetails.setMessageReason("Unable to Registor " + sqlException);
        }
        return responseDetails;
    }

    private void insertDetails(TVSeries requestDetails, ResponseDetails responseDetails) throws SQLException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        String query = getquery(requestDetails);
        int responseCode = statement.executeUpdate(query);
        if (1 == responseCode) {
            responseDetails.setMessageID(String.valueOf(responseCode));
            responseDetails.setMessageReason("Successfully updated details");
        }

    }

    static JSONObject downloadJsonObject(String request) throws JSONException {
        // Build connection
        try {
            // TV Objects
            URL query = new URL(request);
            HttpURLConnection connection = (HttpURLConnection) query.openConnection();

            // Connection
            connection.setDoOutput(true);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type", "application/json");

            //Buffered Reader
            BufferedReader br = new BufferedReader(new InputStreamReader((connection.getInputStream())));
            String output;

            // To JSONObjects
            while ((output = br.readLine()) != null) {
                JSONObject obj = new JSONObject(output);
                return obj;
            }

        } catch (Exception e) {
            System.out.println("Error during downloadJsonObject");
            System.out.println(e);
        }
        return null;
    }

    public JSONObject downloadResultJSON(int id) {

        JSONObject JSONTV = downloadJsonObject("http://api.themoviedb.org/3/tv/" + id + "?api_key="+tmdbKey);
        JSONObject resultJSON = JSONTV.getJSONObject("last_episode_to_air");

        return resultJSON;

    }

    private String getquery(TVSeries requestDetails) {
        JSONObject currentTVSeries = downloadResultJSON(requestDetails.getId());
        String name = currentTVSeries.getString("name");
        String lastUpdate = currentTVSeries.getJSONObject("last_episode_to_air").getString("air_date");
        String nextEpisode = currentTVSeries.getJSONObject("next_episode_to_air").getString("air_date");
        String query = "INSERT INTO SeriesTicker.TVSeries " + "VALUES (" + requestDetails.getId() + ", '" + name + "', 1, 1, '" + lastUpdate + "','" + nextEpisode + "')";
        return query;
    }

    private Connection getConnection() throws SQLException {
        String url = "jdbc:mysql://arn:aws:rds:us-east-2:534767986336:db:seriesticker/seriesticker";
        String username = "";
        String password = "";
        Connection conn = DriverManager.getConnection(url, username, password);
        return conn;
    }

}
