package com.skonuniverse.flink.datatype

import java.sql.Timestamp

case class Message(eventTime: Timestamp,
                   code: Int,
                   value: Double = Double.NegativeInfinity)
