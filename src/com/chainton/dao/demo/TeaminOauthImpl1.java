package com.chainton.dao.demo;

public class TeaminOauthImpl1 implements ITeaminOauth1 {

	@Override
	public boolean existAppleName(String appleName) {

		return executSql(boolean.class, true, "SELECT count(*) from oauth_app_manager where app_name=? ", appleName);
	}

}
