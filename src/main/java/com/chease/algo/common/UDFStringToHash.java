package com.chease.algo.common;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFStringToHash extends UDF{
	
	public String evaluate(String str, Integer bucket) {
			
		if(StringUtils.isBlank(str) || bucket == null) {
			return null;
		}
		
		byte[] digest = null;
	    try {
	    	MessageDigest md5 = MessageDigest.getInstance("md5");
	        digest  = md5.digest(str.getBytes("utf-8"));
	           
	    } catch (NoSuchAlgorithmException e) {
	    	e.printStackTrace();
	    } catch (UnsupportedEncodingException e) {
	    	e.printStackTrace();
	    }
	        
	        
	      // 取mod
	     BigInteger str_hash_num = new BigInteger(1, digest);
	     BigInteger new_bucket = new BigInteger(String.valueOf(bucket));
	        
	     String hash_result = String.valueOf(str_hash_num.mod(new_bucket));
	     hash_result = String.valueOf(Integer.valueOf(hash_result) + 1);
	        
	     return hash_result;		
	}
	
	
	public static String evaluate2(String str, Integer bucket) {
		
		if(StringUtils.isBlank(str) || bucket == null) {
			return null;
		}
		
		byte[] digest = null;
	    try {
	    	MessageDigest md5 = MessageDigest.getInstance("md5");
	        digest  = md5.digest(str.getBytes("utf-8"));
	           
	    } catch (NoSuchAlgorithmException e) {
	    	e.printStackTrace();
	    } catch (UnsupportedEncodingException e) {
	    	e.printStackTrace();
	    }
	        
	        
	      // 取mod
	     BigInteger str_hash_num = new BigInteger(1, digest);
	     BigInteger new_bucket = new BigInteger(String.valueOf(bucket));
	        
	     String hash_result = String.valueOf(str_hash_num.mod(new_bucket));
	     
	     hash_result = String.valueOf(Integer.valueOf(hash_result) + 1);
	        
	     return hash_result;		
	}
	
	
	public static void main(String[] args) {
		System.out.println(evaluate2("abc", 117));
	}

}
