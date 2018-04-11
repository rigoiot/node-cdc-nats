#!/usr/bin/env node

/* jslint node: true */
'use strict';

var proto = require('../lib/proto');


var server = process.argv[2];
var subject = process.argv[3];
var data = process.argv[4] || '';
var contentType = process.argv[5] || 'json';

if (!subject || !server) {
  console.log('Usage: cdc-pub <server> <subject> [msg] [contentType]');
  process.exit();
}

var nats = require('nats').connect({
  servers: [server]
});

nats.on('error', function(e) {
  console.log('Error [' + nats.options.url + ']: ' + e);
  process.exit();
});

var cdcMsg = new proto.CDCMsg();
cdcMsg.setPublisher('cdc-pub');
cdcMsg.setChannel(subject);
cdcMsg.setContentType(contentType);
cdcMsg.setProtocol('nats');
cdcMsg.setData(data);

nats.publish(subject, new Buffer(cdcMsg.serializeBinary()), function() {
  console.log('Published [' + subject + '] : "' + data + '"');
  process.exit();
});
