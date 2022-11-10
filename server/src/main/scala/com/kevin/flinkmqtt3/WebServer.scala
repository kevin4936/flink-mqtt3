package com.kevin.flinkmqtt3

 import java.time.{LocalDateTime}

object WebServer {

  def main(args: Array[String]): Unit = {
    Mqtt3App.start
    val time = LocalDateTime.now()
    val result = time.toString
    println(s"Web Server online at $result")
  }
}
