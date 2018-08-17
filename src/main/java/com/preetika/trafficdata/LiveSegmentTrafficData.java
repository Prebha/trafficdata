package com.preetika.trafficdata;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.json.JSONObject;

@DefaultCoder(AvroCoder.class)
public class LiveSegmentTrafficData {
	private String direction;
	private String fromStreet;
	private String lastUpdate;
	private int segmentId;
	private String street;
	private String toStreet;
	private float length;
	private String streetHeading;
	private String comments;
	private int traffic;
	private float startLatitude;
	private float endLatitude;
	private float endLongitude;
	private float startLongitude;

	public LiveSegmentTrafficData() {
		// constructor for AvroCoder.
	}

	public static LiveSegmentTrafficData fromString(String line) {
		LiveSegmentTrafficData data = new LiveSegmentTrafficData();
		JSONObject json = new JSONObject(line);
		data.direction = JsonHelper.getJsonString(json, "_direction");
		data.fromStreet = JsonHelper.getJsonString(json, "_fromst");
		data.lastUpdate = JsonHelper.getJsonString(json, "_last_updt");
		data.segmentId = Integer.parseInt(JsonHelper.getJsonString(json, "segmentid"));
		data.street = JsonHelper.getJsonString(json, "street");
		data.toStreet = JsonHelper.getJsonString(json, "_tost");
		data.length = Float.parseFloat(JsonHelper.getJsonString(json, "_length"));
		data.streetHeading = JsonHelper.getJsonString(json, "_strheading");
		data.comments = JsonHelper.getJsonString(json, "_comments");
		data.traffic = Integer.parseInt(JsonHelper.getJsonString(json, "_traffic"));
		data.startLatitude = Float.parseFloat(JsonHelper.getJsonString(json, "_lif_lat"));
		data.endLatitude = Float.parseFloat(JsonHelper.getJsonString(json, "_lit_lat"));
		data.endLongitude = Float.parseFloat(JsonHelper.getJsonString(json, "_lit_lon"));
		data.startLongitude = Float.parseFloat(JsonHelper.getJsonString(json, "start_lon"));
		return data;
	}

	public String getDirection() {
		return direction;
	}

	public String getFromStreet() {
		return fromStreet;
	}

	public String getLastUpdate() {
		return lastUpdate;
	}

	public int getSegmentId() {
		return segmentId;
	}

	public String getStreet() {
		return street;
	}

	public String getToStreet() {
		return toStreet;
	}

	public float getLength() {
		return length;
	}

	public String getStreetHeading() {
		return streetHeading;
	}

	public String getComments() {
		return comments;
	}

	public int getTraffic() {
		return traffic;
	}

	public float getStartLatitude() {
		return startLatitude;
	}

	public float getEndLatitude() {
		return endLatitude;
	}

	public float getEndLongitude() {
		return endLongitude;
	}

	public float getStartLongitude() {
		return startLongitude;
	}
}
