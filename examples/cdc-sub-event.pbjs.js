#!/usr/bin/env node

/* jslint node: true */
'use strict';

var pb = require('../lib/proto');

var server = process.argv[2];
var subject = process.argv[3];

if (!subject || !server) {
  console.log('Usage: cdc-sub <server> <subject>');
  process.exit();
}

var nats = require('nats').connect({
  preserveBuffers: true,
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
  var cdcMsg = pb.CDCMsg.decode(msg.toByteArray());
  console.log(new Date() + ' >>>> ' + JSON.stringify(cdcMsg.toJSON()));
  var presenceEvent = pb.CDCClientPresenceEvent.decode(cdcMsg.payload);
  console.log(new Date() + ' >>>> ' + JSON.stringify(presenceEvent.toJSON()));
});
