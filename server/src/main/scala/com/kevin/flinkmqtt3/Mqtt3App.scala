package com.kevin.flinkmqtt3

import com.kevin.flink.streaming.connectors.mqtt.{MQTTMessage, MQTTStreamSink, MQTTStreamSource}
import com.kevin.flink.streaming.connectors.mqtt.internal.{MQTTExceptionListener, RunningChecker}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.TimeUnit

//https://github.com/wuchong/my-flink-project/blob/master/src/main/java/myflink/HotItems.java

object Mqtt3App {

  //private static final String MQTT_SERVER_TCP = "tcp://127.0.0.1:1883";
  private val MQTT_SERVER_TCP: String = "tcp://127.0.0.1:1883"
  private val POST_MESSAGE: String = "I am flink mqtt message."
  private val CALL_BACK_MESSAGE: String = "I am flink mqtt callback message."
  private val LOG = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)

  def start: Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))
    env.setParallelism(1)
    env.enableCheckpointing(5000)
    val args: Array[String] = Array("--flink.mqtt.connection.cache.timeout", "150000", "--flink.mqtt.client.publish.attempts", "-1", "--flink.mqtt.client.publish.backoff", "10000")
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(parameterTool)
    val parameters: ExecutionConfig.GlobalJobParameters = env.getConfig.getGlobalJobParameters
    val map: util.Map[String, String] = parameters.toMap
    val timeout: String = map.get("flink.mqtt.connection.cache.timeout")
    val attempts: String = map.get("flink.mqtt.client.publish.attempts")
    val backoff: String = map.get("flink.mqtt.client.publish.backoff")
    System.setProperty("flink.mqtt.connection.cache.timeout", timeout)
    System.setProperty("flink.mqtt.client.publish.attempts", attempts)
    System.setProperty("flink.mqtt.client.publish.backoff", backoff)

    val password: String = "test_password"
    //String salt = "secret";
    //String passwordSecret = Hashing.sha256().newHasher().putString(password+salt, Charsets.UTF_8).hash().toString() ;
    val sourceConfig = new util.HashMap[String, String]
    sourceConfig.put("brokerUrl", MQTT_SERVER_TCP)
    sourceConfig.put("clientId", "flinkMqttSource")
    sourceConfig.put("topic", "flink-mqtt-source")
    sourceConfig.put("username", "test")
    sourceConfig.put("password", password)
    sourceConfig.put("QoS", "2")
    sourceConfig.put("persistence", "memory")
    //sourceConfig.put("localStorage", "/e/mqtt-message");

    val sinkConfig = new util.HashMap[String, String]
    sinkConfig.put("brokerUrl", MQTT_SERVER_TCP)
    sinkConfig.put("clientId", "flinkMqttSink")
    sinkConfig.put("topic", "flink-mqtt-sink")
    sinkConfig.put("username", "test2")
    sinkConfig.put("password", password)
    sinkConfig.put("QoS", "2")
    sinkConfig.put("persistence", "memory")

    val source: MQTTStreamSource = new MQTTStreamSource(sourceConfig)
    source.setLogFailuresOnly(true)
    val exceptionListener: MQTTExceptionListener = new MQTTExceptionListener(LOG, true)
    source.setExceptionListener(exceptionListener)
    source.setRunningChecker(new RunningChecker)
    val stream: DataStream[MQTTMessage] = env.addSource(source)
    val dataStream: DataStream[MQTTMessage] = stream.flatMap(new FlatMapFunction[MQTTMessage, MQTTMessage]() {
      @throws[Exception]
      override def flatMap(message: MQTTMessage, out: Collector[MQTTMessage]): Unit = {
        assert(new String(message.getPayload) == POST_MESSAGE)
        message.setPayload(CALL_BACK_MESSAGE.getBytes(StandardCharsets.UTF_8))
        out.collect(message)
      }
    }).setParallelism(1)
    val configuration: Configuration = new Configuration
    ExecutionEnvironment.getExecutionEnvironment.getConfig.setGlobalJobParameters(configuration)
    val streamSink: MQTTStreamSink = new MQTTStreamSink(sinkConfig)
    dataStream.addSink(streamSink)
    //dataStream.print();
    env.execute
  }

}
