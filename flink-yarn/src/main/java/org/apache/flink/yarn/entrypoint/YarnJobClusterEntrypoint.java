/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.DynamicParametersConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.IOException;
import java.util.Map;

/**
 * $JAVA_HOME/bin/java
 * -Xmx1073741824
 * -Xms1073741824
 * -XX:MaxMetaspaceSize=268435456
 * -Dlog.file="<LOG_DIR>/jobmanager.log"
 * -Dlog4j.configuration=file:log4j.properties
 * -Dlog4j.configurationFile=file:log4j.properties
 *
 * org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint
 *
 * -D jobmanager.memory.off-heap.size=134217728b
 * -D jobmanager.memory.jvm-overhead.min=201326592b
 * -D jobmanager.memory.jvm-metaspace.size=268435456b
 * -D jobmanager.memory.heap.size=1073741824b
 * -D jobmanager.memory.jvm-overhead.max=201326592b 1> <LOG_DIR>/jobmanager.out 2> <LOG_DIR>/jobmanager.err
 */


/**
 * Entry point for Yarn per-job clusters.
 */
public class YarnJobClusterEntrypoint extends JobClusterEntrypoint {

	public YarnJobClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected String getRPCPortRange(Configuration configuration) {
		return configuration.getString(YarnConfigOptions.APPLICATION_MASTER_PORT);
	}

	@Override
	protected DefaultDispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) throws IOException {
		//todo
		return DefaultDispatcherResourceManagerComponentFactory.createJobComponentFactory(
			//YarnResourceManagerFactory extends ActiveResourceManagerFactory
			YarnResourceManagerFactory.getInstance(),
			FileJobGraphRetriever.createFrom(
					configuration,
					YarnEntrypointUtils.getUsrLibDir(configuration).orElse(null)));
	}

	// ------------------------------------------------------------------------
	//  The executable entry point for the Yarn Application Master Process
	//  for a single Flink job.
	// ------------------------------------------------------------------------

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, YarnJobClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		Map<String, String> env = System.getenv();

		final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
		Preconditions.checkArgument(
			workingDirectory != null,
			"Working directory variable (%s) not set",
			ApplicationConstants.Environment.PWD.key());

		try {
			YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
		} catch (IOException e) {
			LOG.warn("Could not log YARN environment information.", e);
		}

		final Configuration dynamicParameters = ClusterEntrypointUtils.parseParametersOrExit(
			args,
			new DynamicParametersConfigurationParserFactory(),
			YarnJobClusterEntrypoint.class);
		// 1.构建配置
		final Configuration configuration = YarnEntrypointUtils.loadConfiguration(workingDirectory, dynamicParameters, env);
		// 2.构建YarnJobClusterEntrypoint
		YarnJobClusterEntrypoint yarnJobClusterEntrypoint = new YarnJobClusterEntrypoint(configuration);
		//todo 3.启动
		ClusterEntrypoint.runClusterEntrypoint(yarnJobClusterEntrypoint);
	}
}
