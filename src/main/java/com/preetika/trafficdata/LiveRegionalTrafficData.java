package com.preetika.trafficdata;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.json.JSONObject;

@DefaultCoder(AvroCoder.class)
public class LiveRegionalTrafficData {

	private float currentSpeed;
	private float east;
	private String last_updt;
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
		data.currentSpeed = Float.parseFloat(jsonObject.getString("current_speed"));
		data.east = Float.parseFloat(jsonObject.getString("_east"));
		data.last_updt = jsonObject.getString("_last_updt");
		data.regionId = jsonObject.getString("_region_id");
		data.north = Float.parseFloat(jsonObject.getString("_north"));
		data.south = Float.parseFloat(jsonObject.getString("_south"));
		data.region = jsonObject.getString("region");
		data.west = Float.parseFloat(jsonObject.getString("_west"));
		data.description = jsonObject.getString("_description");
		return data;
	}

	public float getCurrentSpeed() {
		return currentSpeed;
	}

	public float getEast() {
		return east;
	}

	public String getLast_updt() {
		return last_updt;
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
