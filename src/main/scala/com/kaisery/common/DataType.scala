package com.kaisery.common

object DataType extends Enumeration {
    val TEXT = Value(0, "text")
    val INTEGER = Value("integer")
    val DOUBLE = Value("double")
    val EMAIL = Value("email")
    val FAKE_EMAIL = Value("fake email")
    val POST_CODE = Value("post code")
    val IP_V4 = Value("ipv4")
    val FAKE_IP_V4 = Value("fake ipv4")
    val UUID = Value("uuid")
    val DATE = Value("date")
    val TIME = Value("time")
}
