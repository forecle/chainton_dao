package com.chainton.dao;

import com.chainton.dao.demo.ITeaminOauth1;
import com.chainton.dao.demo.TeaminOauthImpl1;

public class T1 {

	public static void main(String[] args) {

		DatabaseConnection.newInstance().init(
				"jdbc:mysql://localhost:3306/teamin?useUnicode=true&characterEncoding=UTF-8&noAccessToProcedureBodies=true",
				"teaminweb", "teaminweb");
		
		DaoManager.put("aouth", new TeaminOauthImpl1());
		
		System.out.println(((ITeaminOauth1)DaoManager.getDao("aouth")).existAppleName("sdf") );
		
		System.out.println(DatabaseConnection.getConnection());
	}
}
