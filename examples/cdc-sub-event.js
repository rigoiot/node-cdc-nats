#!/usr/bin/env node

/* jslint node: true */
'use strict';

var proto = require('../lib/proto');

var server = process.argv[2];
var subject = process.argv[3];

if (!subject || !server) {
  console.log('Usage: cdc-sub <server> <subject>');
  process.exit();
}

var nats = require('nats').connect({
  servers: [server]
});

nats.on('error', function(e) {
  console.log('Error [' + nats.options.url + ']: ' + e);
  process.exit();
});

nats.on('close', function() {
  console.log('CLOSED');
  process.exit();
});

console.log('Listening on [' + subject + ']');

nats.subscribe(subject, function(msg) {
  console.log(msg);
  var cdcMsg = proto.CDCMsg.deserializeBinary(
    new Buffer(msg, 'binary').toByteArray()
  );
  var presenceEvent = proto.CDCClientPresenceEvent.deserializeBinary(
    cdcMsg.getPayload()
  );
  console.log('Received "' + presenceEvent + '"');
});