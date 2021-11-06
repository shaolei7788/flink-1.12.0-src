package org.apache.flink.client;

import org.apache.flink.client.cli.CliFrontend;

public class CliFrontendTest {
	public static void main(String[] args) {

		String [] param = new String[8] ;
		param[0] = "run" ;
		param[1] = "-t" ;
		param[2] = "yarn-per-job" ;
		param[3] = "-c" ;
		param[4] = "com.qingcloud.flink.base.WordCount" ;
		param[5] = "G:\\Workspaces\\fink-1-12\\target\\com.qingcloud-1.0-SNAPSHOT.jar" ;
		param[6] = "--port" ;
		param[7] = "9999" ;

		CliFrontend.main(param);

	}
}
