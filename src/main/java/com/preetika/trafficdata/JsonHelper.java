package com.preetika.trafficdata;

import org.json.JSONObject;

public final class JsonHelper {

	public static String getJsonString(JSONObject json, String key) {
		try {
			return json.getString(key);
		} catch (Exception e) {
			return "";
		}
	}

	private JsonHelper() {
		// hiding the constructor.
	}

}
