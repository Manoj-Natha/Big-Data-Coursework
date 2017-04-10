package JavaHDFS.JavaHDFS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class twitterHashtagAnalysis {

	// Encodes the consumer key and secret to create the basic authorization key
	private static String encodeKeys(String consumerKey, String consumerSecret) {
		try {
			String encodedConsumerKey = URLEncoder.encode(consumerKey, "UTF-8");
			String encodedConsumerSecret = URLEncoder.encode(consumerSecret, "UTF-8");

			String fullKey = encodedConsumerKey + ":" + encodedConsumerSecret;
			byte[] encodedBytes = Base64.encodeBase64(fullKey.getBytes());			
			return new String(encodedBytes);
		} catch (UnsupportedEncodingException e) {
			return new String();
		}
	}

	// Constructs the request for requesting a bearer token and returns that
	// token as a string
	private static String requestBearerToken(String endPointUrl) throws IOException {
		HttpsURLConnection connection = null;
		String encodedCredentials = encodeKeys("DWdgGr1eyk3pElt1DPhm3godC",
				"7vD5ogM2wIibSDMMf7eedrIQOPonySo7j5tgOTONybEn9nW5aN");
		try {
			URL url = new URL(endPointUrl);
			connection = (HttpsURLConnection) url.openConnection();
			connection.setDoOutput(true);
			connection.setDoInput(true);
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Host", "api.twitter.com");
			connection.setRequestProperty("User-Agent", "twitterHashtagAnalysis");
			connection.setRequestProperty("Authorization", "Basic " + encodedCredentials);
			connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
			connection.setRequestProperty("Content-Length", "29");
			connection.setUseCaches(false);

			writeRequest(connection, "grant_type=client_credentials");

			// Parse the JSON response into a JSON mapped object to fetch fields
			// from.
			JSONObject obj = (JSONObject) JSONValue.parse(readResponse(connection));
			if (obj != null) {
				String tokenType = (String) obj.get("token_type");
				String token = (String) obj.get("access_token");
				return ((tokenType.equals("bearer")) && (token != null)) ? token : "";
			}
			return new String();
		} catch (MalformedURLException e) {
			throw new IOException("Invalid endpoint URL specified.", e);
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	// Fetches the first tweet from a given user's timeline
	private static void searchTweet(String endPointUrl, String date) throws IOException {
		HttpsURLConnection connection = null;
		String bearerToken = requestBearerToken("https://api.twitter.com/oauth2/token");
		try {
			URL url = new URL(endPointUrl);
			connection = (HttpsURLConnection) url.openConnection();
			connection.setDoOutput(true);
			connection.setDoInput(true);
			connection.setRequestMethod("GET");
			connection.setRequestProperty("Host", "api.twitter.com");
			connection.setRequestProperty("User-Agent", "twitterHashtagAnalysis");
			connection.setRequestProperty("Authorization", "Bearer " + bearerToken);
			connection.setUseCaches(false);

			// Parse the JSON response into a JSON mapped object to fetch fields
			// from.

			JSONObject v = (JSONObject) JSONValue.parse(readResponse(connection));
			JSONArray obj = (JSONArray) v.get("statuses");

			// Writing Twitter data to file
			Configuration conf = new Configuration();
			String fileName = "tweets-" + date + ".txt";
			FileSystem fs = FileSystem.get(URI.create(fileName), conf);
			OutputStream out = fs.create(new Path(fileName), new Progressable() {
				public void progress() {
					System.out.print(".");
				}
			});

			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));

			if (obj != null) {
				for (Object obj2 : obj) {

					// System.out.println("@"+((JSONObject)((JSONObject)obj2).get("user")).get("screen_name").toString()+"--->");
					// System.out.println(((JSONObject)obj2).get("text").toString());
					br.write("@" + ((JSONObject) ((JSONObject) obj2).get("user")).get("screen_name").toString() + "--->"
							+ ((JSONObject) obj2).get("text").toString() +"\n\n");
				}

				br.close();
				fs.close();

			}

		} catch (MalformedURLException e) {
			throw new IOException("Invalid endpoint URL specified.", e);
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	// Writes a request to a connection
	private static boolean writeRequest(HttpsURLConnection connection, String textBody) {
		try {
			BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream()));
			wr.write(textBody);
			wr.flush();
			wr.close();

			return true;
		} catch (IOException e) {
			return false;
		}
	}

	// Reads a response for a given connection and returns it as a string.
	private static String readResponse(HttpsURLConnection connection) {
		try {
			StringBuilder str = new StringBuilder();

			BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String line = "";
			while ((line = br.readLine()) != null) {
				str.append(line + System.getProperty("line.separator"));
			}
			return str.toString();
		} catch (IOException e) {
			return new String();
		}
	}

	public static void main(String[] args) throws IOException {

		Calendar cal = Calendar.getInstance();

		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		Date d = cal.getTime();
		String dates[] = new String[7];
		dates[6] = ft.format(d);
		for (int i = 5; i >= 0; i--) {

			cal.add(Calendar.DATE, -1);
			d = cal.getTime();
			dates[i] = ft.format(d);
		}

		for (int i = 0; i < 6; i++)
			searchTweet("https://api.twitter.com/1.1/search/tweets.json?q=utd&since=" + dates[i] + "&" + "until="
					+ dates[i + 1] + "&" + "count=100", dates[i]);

	}

}
