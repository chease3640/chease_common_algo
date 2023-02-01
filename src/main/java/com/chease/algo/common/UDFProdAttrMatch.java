package com.chease.algo.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.ahocorasick.trie.Emit;
import org.ahocorasick.trie.Trie;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;


public class UDFProdAttrMatch extends UDF{
	// attr tree
	static public Trie attTrie = new Trie();
	// 类目属性map
	static private Map<Long, Set<String>> cateAttrMap = new HashMap<Long, Set<String>>();
	
	// key:属性值; value: map, key为类目id，value属性项list
	static private Map<String, Map<Long, List<String>>> attrCateMap = new HashMap<String, Map<Long, List<String>> >();
	
	static {
		// URL url = UDFProdAttrMatch.class.getResource("/product_property_info.txt"); 
		// String filename = url.getPath(); 
		InputStreamReader inputStreamReader = null;
		BufferedReader reader = null;
		
		Set<String> attrValueSet = new HashSet<String>();
		try {
			// reader = new BufferedReader(new FileReader(filename));
			inputStreamReader = new InputStreamReader(new Object(){}.getClass().getClassLoader().getResourceAsStream("product_property_info.txt"), "UTF-8");
			reader = new BufferedReader(inputStreamReader); 
			String  line = reader.readLine(); 
			
			while ((line = reader.readLine())!= null){ 
				String[] strs = line.split("\t");
				// 22507	闭合方式	拉链式,扣式,包盖式,系绳,魔术贴
				long cateId = Long.valueOf(strs[0]);
				String pName = strs[1];
				
				if(strs.length == 3) {
					String[] tmpValues = strs[2].split(",");
					for(String tmpValue : tmpValues) {
						attrValueSet.add(tmpValue);
						
						// attrCateMap init
						if(attrCateMap.containsKey(tmpValue)) {
							Map<Long, List<String>> tmpMap = attrCateMap.get(tmpValue);
							if(tmpMap.containsKey(cateId)) {
								//tmpMap.get(cateId).add(pName);
								List<String> tmList = tmpMap.get(cateId);
								tmList.add(pName);
							}else {
								//tmpMap.put(cateId, Arrays.asList(pName));
								tmpMap.put(cateId, new ArrayList<String>(Arrays.asList(pName)));
							}
						}else {
							Map<Long, List<String>> tmpMap = new HashMap<>();
							// tmpMap.put(cateId, Arrays.asList(pName));
							
							tmpMap.put(cateId, new ArrayList<String>(Arrays.asList(pName)));
							
							attrCateMap.put(tmpValue, tmpMap);
						}
					}
					// cateAttrMap init 
					cateAttrMap.put(Long.valueOf(strs[0]), attrValueSet);
				}
			} 
			//init tree
			for(String tmpValue : attrValueSet) {
				attTrie.addKeyword(tmpValue.toUpperCase());
			}
			reader.close(); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public String evaluate(String text, Long leafCateId) {
		if(StringUtils.isBlank(text) || leafCateId == null) {
			return null;
		}
		
		if(!cateAttrMap.containsKey(leafCateId)) {
			return null;
		}
		
		Set<String> tarCateAttrSet = cateAttrMap.get(leafCateId);
		Collection<Emit> emits = attTrie.parseText(text.toUpperCase());
		//System.out.println(emits);
		
		StringBuffer rStringBuffer = new StringBuffer();
		for(Emit emit : emits) {
			if(!tarCateAttrSet.contains(emit.getKeyword())) {
				continue;
			}
			
			String matchWord = emit.getKeyword();
			if(matchWord.length() <= 1) {
				continue;
			}
			if(attrCateMap.containsKey(matchWord)
					&& attrCateMap.get(matchWord).containsKey(leafCateId)
					) {
				List<String> tarPList = attrCateMap.get(matchWord).get(leafCateId);
				for(String pName : tarPList) {
					rStringBuffer.append(pName).append("=").append(matchWord).append(",");
				}
			}
		}
		
		if(rStringBuffer.length() > 0) {
			return rStringBuffer.substring(0, rStringBuffer.length()-1);
		}
		return null;
	}
	
	public static void main(String[] args) throws IOException {
		UDFProdAttrMatch ins = new UDFProdAttrMatch();
		String text = "这羽绒服耐磨性怎么样 是";
		String rString= ins.evaluate(text, 26798L);
		System.out.println(rString);
	
	}

}
