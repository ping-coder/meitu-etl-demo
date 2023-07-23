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

package ping.code.meitu;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink;


import org.apache.flink.util.ChildFirstClassLoader;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.util.List;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		SourceFunction<String> mySqlSource = MySqlSource.<String>builder()
				.hostname("00.00.00.00")
				.port(3306)
				.databaseList("meitu_db") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
				.tableList("meitu_db.t_user_stat") // set captured table
				.username("db")
				.password("password")
				.deserializer(new MyJsonDeserializationSchemaFunction()) // converts SourceRecord to JSON String
				.startupOptions(StartupOptions.initial())
				.build();

		SinkFunction<String> pubsubSink = PubSubSink.newBuilder()
				.withSerializationSchema(new SimpleStringSchema())
				.withProjectName("peace-demo")
				.withTopicName("meitu-pub")
				.withCredentials(ServiceAccountCredentials.fromStream(getAutheficateFile()))
				.build();

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// enable checkpoint
		env.enableCheckpointing(30000);

		env.addSource(mySqlSource)
				.addSink(pubsubSink);

		// execute program
		env.execute("Flink CDC from MySQL to PubSub");
	}

	public static InputStream getAutheficateFile() {
		String configFile = "/peace_demo_meitu_flink_key.json";
		InputStream credentialsFile = StreamingJob.class.getResourceAsStream(configFile);
		return credentialsFile;
	}

	public static class MyJsonDeserializationSchemaFunction implements DebeziumDeserializationSchema<String> {
		@Override
		public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
			Struct value = (Struct) sourceRecord.value();
			Struct source = value.getStruct("source");

			//获取数据库名称
			String db = source.getString("db");
			String table = source.getString("table");

			//获取数据类型
			String type = Envelope.operationFor(sourceRecord).toString().toLowerCase();
			if(type.equals("create")){
				type = "insert";
			}
			String message =
							"database:" + db + "," +
							"table:" + table + "," +
							"type:" + type;

			//获取数据data
			Struct after = value.getStruct("after");
			List<Field> fields = after.schema().fields();
			for (Field field : fields) {
				String field_name = field.name();
				Object fieldValue  = after.get(field);
				message += "," + field_name + ":" + fieldValue;
			}
			//向下游传递数据
			collector.collect(message);
		}

		@Override
		public TypeInformation<String> getProducedType() {
			return TypeInformation.of(String.class);
		}
	}
}