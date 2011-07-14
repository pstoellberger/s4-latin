package io.s4.examples.logstats.pe;

import io.s4.latin.pojo.StreamRow;
import io.s4.persist.Persister;
import io.s4.processor.AbstractPE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class TopNResourcesPE extends AbstractPE {
	private String id;
	private Persister persister;
	private int entrysum = 10;
	private Map<String, Double> resourceMap = new ConcurrentHashMap<String, Double>();
	private int persistTime;
	private String persistKey = "myapp:topNResources";

	public void setId(String id) {
		this.id = id;
	}

	public Persister getPersister() {
		return persister;
	}

	public void setPersister(Persister persister) {
		this.persister = persister;
	}

	public int getEntrysum() {
		return entrysum;
	}

	public void setEntrysum(int entrysum) {
		this.entrysum = entrysum;
	}

	public int getPersistTime() {
		return persistTime;
	}

	public void setPersistTime(int persistTime) {
		this.persistTime = persistTime;
	}

	public String getPersistKey() {
		return persistKey;
	}

	public void setPersistKey(String persistKey) {
		this.persistKey = persistKey;
	}


	public void processEvent(StreamRow row) {
		System.out.println(row);
		String resource = (String) row.get("resource");
		Double bytes = Double.parseDouble((String) row.get("bytes"));
		bytes = bytes/1024/1024;
		if (resourceMap.containsKey(resource)) {
			bytes = resourceMap.get(resource) + bytes;
		}
		resourceMap.put(resource, bytes);
	}

	public ArrayList<TopNEntry> getTopTopics() {
		if (entrysum < 1) 
			return null;

		ArrayList<TopNEntry> sortedList = new ArrayList<TopNEntry>();

		for (String key : resourceMap.keySet()) {
			sortedList.add(new TopNEntry(key, resourceMap.get(key)));
		}

		Collections.sort(sortedList);

		// truncate: Yuck!!
		// unfortunately, Kryo cannot deserialize RandomAccessSubList
		// if we use ArrayList.subList(...)
		while (sortedList.size() > entrysum)
			sortedList.remove(sortedList.size() - 1);

		return sortedList;
	}

	@Override
	public void output() {
		List<TopNEntry> sortedList = new ArrayList<TopNEntry>();

		System.out.println("--------- RESOURCE: " + resourceMap.keySet().size());
		for (String key : resourceMap.keySet()) {
			sortedList.add(new TopNEntry(key, resourceMap.get(key)));
//			System.out.println(key + " \t\t Size: " + resourceMap.get(key) );
		}

		Collections.sort(sortedList);

		try {
			JSONObject message = new JSONObject();
			JSONArray jsonTopN = new JSONArray();

			for (int i = 0; i < entrysum; i++) {
				if (i == sortedList.size()) {
					break;
				}
				TopNEntry tne = sortedList.get(i);
				JSONObject jsonEntry = new JSONObject();
				jsonEntry.put("resource", tne.getResource());
				jsonEntry.put("sum", Math.floor(tne.getSum()) + " MB");
				jsonTopN.put(jsonEntry);
				System.out.print(tne.getResource() + "\t" + Math.floor(tne.getSum()) + " MB\t");
			}
			message.put("topN", jsonTopN);
			persister.set(persistKey, message.toString()+"\n\n", persistTime);
		} catch (Exception e) {
			Logger.getLogger("s4").error(e);
		}
	}

	@Override
	public String getId() {
		return this.id;
	}

	public static class TopNEntry implements Comparable<TopNEntry> {
		public TopNEntry(String resource, Double sum) {
			this.resource = resource;
			this.sum = sum;
		}

		public TopNEntry() {}

		String resource = null;
		Double sum = 0.0;

		public String getResource() {
			return resource;
		}

		public void setTopic(String resource) {
			this.resource = resource;
		}

		public Double getSum() {
			return sum;
		}

		public void setSum(Double sum) {
			this.sum = sum;
		}

		public int compareTo(TopNEntry topNEntry) {
			Double diff = Math.floor(this.sum - topNEntry.getSum());
			if (diff < 0.0) {
				return 1;
			} else if (diff > 0.0) {
				return -1;
			}
			return 0;
		}

		public String toString() {
			return "resource:" + resource + " sum:" + sum;
		}
	}
}