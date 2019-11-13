#!/usr/bin/env node

/* jslint node: true */
"use strict";

var server = process.argv[2];
var topic = process.argv[3];
var reqSub = process.argv[4];
var resSubs = process.argv[5];
var data = process.argv[6];

if (!reqSub || !resSubs || !server || !topic) {
  console.log("Usage: cdc-rpc <server> <topic> <reqSub> <resSubs>");
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

console.log("RPC on [" + reqSub + "]");

nats.rpc(reqSub, resSubs.split(","), data, 10).then(function(data) {
  console.log(data);
});
