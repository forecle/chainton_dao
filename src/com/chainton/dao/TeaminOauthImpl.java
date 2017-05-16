package com.chainton.dao;

public class TeaminOauthImpl implements ITeaminOauth {

	@Override
	public boolean existAppleName(String appleName) {

		return executSql(boolean.class, true, "SELECT count(*) from oauth_app_manager where app_name=? ", appleName);
	}

}
