#!/usr/bin/env node

/* jslint node: true */
'use strict';

var server = process.argv[2];
var topic = process.argv[3];
var subject = process.argv[4];
var data = process.argv[5];

if (!subject || !server || !topic || !data) {
  console.log('Usage: cdc-pub <server> <topic> <subject> <data>');
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

kafka.publish(subject, data, function(error, data) {
  console.log(error);
  console.log(data);
});