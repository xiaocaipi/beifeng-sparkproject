package com.ibeifeng.sparkproject.domain;

import java.io.Serializable;

/**
 * 广告黑名单
 * @author Administrator
 *
 */
public class AdBlacklist implements Serializable {

	private long userid;

	public long getUserid() {
		return userid;
	}
	public void setUserid(long userid) {
		this.userid = userid;
	}
	
}
