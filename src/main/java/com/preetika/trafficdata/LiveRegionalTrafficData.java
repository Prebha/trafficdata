package com.preetika.trafficdata;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.json.JSONObject;

@DefaultCoder(AvroCoder.class)
public class LiveRegionalTrafficData {

	private float currentSpeed;
	private float east;
	private String lastUpdate;
	private String regionId;
	private float north;
	private float south;
	private String region;
	private float west;
	private String description;

	public LiveRegionalTrafficData() {
		// constructor for AvroCoder.
	}

	public static LiveRegionalTrafficData fromString(String line) {
		JSONObject jsonObject = new JSONObject(line);
		LiveRegionalTrafficData data = new LiveRegionalTrafficData();
		data.currentSpeed = Float.parseFloat(JsonHelper.getJsonString(jsonObject, "current_speed"));
		data.east = Float.parseFloat(JsonHelper.getJsonString(jsonObject, "_east"));
		data.lastUpdate = JsonHelper.getJsonString(jsonObject, "_last_updt");
		data.regionId = JsonHelper.getJsonString(jsonObject, "_region_id");
		data.north = Float.parseFloat(JsonHelper.getJsonString(jsonObject, "_north"));
		data.south = Float.parseFloat(JsonHelper.getJsonString(jsonObject, "_south"));
		data.region = JsonHelper.getJsonString(jsonObject, "region");
		data.west = Float.parseFloat(JsonHelper.getJsonString(jsonObject, "_west"));
		data.description = JsonHelper.getJsonString(jsonObject, "_description");
		return data;
	}

	public float getCurrentSpeed() {
		return currentSpeed;
	}

	public float getEast() {
		return east;
	}

	public String getLast_updt() {
		return lastUpdate;
	}

	public String getRegionId() {
		return regionId;
	}

	public float getNorth() {
		return north;
	}

	public float getSouth() {
		return south;
	}

	public String getRegion() {
		return region;
	}

	public float getWest() {
		return west;
	}

	public String getDescription() {
		return description;
	}
}
