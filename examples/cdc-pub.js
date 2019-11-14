#!/usr/bin/env node

/* jslint node: true */
"use strict";

var proto = require("node-cdc-proto");
var server = process.argv[2];
var topic = process.argv[3];
var subject = process.argv[4];
var data = process.argv[5];

if (!subject || !server || !topic || !data) {
  console.log("Usage: cdc-pub <server> <topic> <subject> <data>");
  process.exit();
}

var nats = require("../lib/nats").connect(
  {
    url: server
  },
  topic
);

nats.on("error", function(e) {
  console.log("Error [" + server + "]: " + e);
  process.exit();
});

console.log("Publish to  [" + subject + "]");

var cdcMsg = proto.CDCMsg.fromObject({
  publisher: "nms",
  channel: subject,
  contentType: "json",
  protocol: "http",
  reply: "",
  QOS: 0,
  retain: false,
  payload: new Uint8Array(new Buffer(data, "utf8"))
});

nats.publish(subject, proto.CDCMsg.encode(cdcMsg).finish(), function(
  error,
  data
) {
  console.log(error);
  console.log(data);
});
