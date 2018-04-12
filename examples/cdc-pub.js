#!/usr/bin/env node

/* jslint node: true */
'use strict';

var server = process.argv[2];
var topic = process.argv[3];
var subject = process.argv[4];

if (!subject || !server || !topic) {
  console.log('Usage: cdc-kafka-sub <server> <topic> <subject>');
  process.exit();
}

var kafka = require('../lib/kafka').connect({
  kafkaHost: server
}, topic);

kafka.on('error', function(e) {
  console.log('Error [' + server + ']: ' + e);
  process.exit();
});

console.log('Publish to  [' + subject + ']');

kafka.publish(subject, 'test', function(error, data) {
  console.log(error);
  console.log(data);
});