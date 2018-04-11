/* jslint node: true */
'use strict';

var kafka = require('kafka-node'),
  proto = require('node-cdc-proto'),
  util = require('util'),
  events = require('events'),
  MQTTPattern = require("mqtt-pattern"),
  uuid = require('uuid');

// Consts
var VERSION = '0.1.0';

// Errors
var BAD_SUBJECT = 'BAD_SUBJECT',
  BAD_SUBJECT_MSG = 'Subject must be supplied',
  BAD_MSG = 'BAD_MSG',
  BAD_MSG_MSG = 'Message can\'t be a function',
  CONN_CLOSED = 'CONN_CLOSED',
  CONN_CLOSED_MSG = 'Connection closed',
  BAD_JSON = 'BAD_JSON',
  BAD_JSON_MSG = 'Message should be a JSON object',
  INVALID_ENCODING = 'INVALID_ENCODING';

var options = {
  // host: 'zookeeper:2181',  // zookeeper host omit if connecting directly to broker (see kafkaHost below)
  kafkaHost: '127.0.0.1:8082', // connect directly to kafka broker (instantiates a KafkaClient)
  groupId: 'node-cdc-kafka-group',
  sessionTimeout: 15000,
  // An array of partition assignment protocols ordered by preference.
  // 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
  protocol: ['roundrobin'],

  // Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved)
  // equivalent to Java client's auto.offset.reset
  fromOffset: 'latest', // default

  // Encoding for value decode
  encoding: 'buffer',

  // Encoding for key decode 
  keyEncoding: 'utf8',

  // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
  outOfRangeOffset: 'earliest', // default
  migrateHLC: false, // for details please see Migration section below
  migrateRolling: true,
  // Callback to allow consumers with autoCommit false a chance to commit before a rebalance finishes
  // isAlreadyMember will be false on the first connection, and true on rebalances triggered after that
  onRebalance: (isAlreadyMember, callback) => {
    callback();
  } // or null
};

function KafkaError(message, code, chainedError) {
  Error.captureStackTrace(this, this.constructor);
  this.name = this.constructor.name;
  this.message = message;
  this.code = code;
  this.chainedError = chainedError;
}

util.inherits(KafkaError, Error);
exports.KafkaError = KafkaError;

// Error codes
exports.BAD_SUBJECT = BAD_SUBJECT;
exports.BAD_MSG = BAD_MSG;
exports.CONN_CLOSED = CONN_CLOSED;
exports.BAD_JSON = BAD_JSON;

exports.version = VERSION;

// Define class Kafka
function Kafka(opts, topic) {
  var client = this;
  events.EventEmitter.call(this);
  this.ssid = -1;
  this.subs = {};
  if (opts.groupId === undefined) {
    opts.groupId = uuid.v4()
  }
  this.consumerGroup = new kafka.ConsumerGroup(Object.assign({}, options, opts), topic || 'cdc.client')
  this.consumerGroup.on('error', function(error) {
    client.processErr(error);
  });
  this.consumerGroup.on('message', function(message) {
    client.processMsg(message)
  });
}

// Close the connection to the server.
Kafka.prototype.close = function() {
  this.closed = true;
  this.removeAllListeners();
  this.ssid = -1;
  this.subs = null;
};

exports.connect = function(opts, topic) {
  return new Kafka(opts, topic);
};

util.inherits(Kafka, events.EventEmitter);

Kafka.prototype.processErr = function(error) {
  this.emit('error', new KafkaError(error.message));
};

Kafka.prototype.processMsg = function(message) {
  // Decode cdc message
  var cdcMsg = proto.CDCMsg.decode(message.value.toByteArray());
  // loop all subs which subject math key
  for (var sid in this.subs) {
    var sub = this.subs[sid];
    if (!MQTTPattern.matches(sub.subject, message.key)) {
      return;
    }
    sub.received += 1;
    // Check for auto-unsubscribe
    if (sub.max !== undefined) {
      if (sub.received === sub.max) {
        delete this.subs[sid];
        this.emit('unsubscribe', sid, sub.subject);
      } else if (sub.received > sub.max) {
        this.unsubscribe(sid);
        sub.callback = null;
      }
    }

    if (sub.callback) {
      sub.callback(cdcMsg, sub.subject, sid);
    }
  }
}

// Subscribe to a given subject, with optional options and callback.
Kafka.prototype.subscribe = function(subject, opts, callback) {
  if (this.closed) {
    throw (new KafkaError(CONN_CLOSED_MSG, CONN_CLOSED));
  }

  var max;
  if (typeof opts === 'function') {
    callback = opts;
    opts = undefined;
  } else if (opts && typeof opts === 'object') {
    // FIXME, check exists, error otherwise..
    max = opts.max;
  }

  this.ssid += 1;
  this.subs[this.ssid] = {
    'subject': subject,
    'callback': callback,
    'received': 0
  };

  this.emit('subscribe', this.ssid, subject, opts);

  if (max) {
    this.unsubscribe(this.ssid, max);
  }
  return this.ssid;
}

// Unsubscribe to a given Subscriber Id, with optional max parameter.
Kafka.prototype.unsubscribe = function(sid, opt_max) {
  if (sid < 0 || sid == undefined || this.closed) {
    return;
  }

  var sub = this.subs[sid];
  if (sub === undefined) {
    return;
  }
  sub.max = opt_max;
  if (sub.max === undefined || (sub.received >= sub.max)) {
    delete this.subs[sid];
    this.emit('unsubscribe', sid, sub.subject);
  }
}

// Kafka.prototype.publish = function(subject, msg, opt_callback) {
//   // They only supplied a callback function.
//   if (typeof subject === 'function') {
//     opt_callback = subject;
//     subject = undefined;
//   }
//   if (!msg) {
//     msg = EMPTY;
//   }
//   if (!subject) {
//     if (opt_callback) {
//       opt_callback(new KafkaError(BAD_SUBJECT_MSG, BAD_SUBJECT));
//     } else {
//       throw (new KafkaError(BAD_SUBJECT_MSG, BAD_SUBJECT));
//     }
//   }
//   if (typeof msg === 'function') {
//     if (opt_callback) {
//       opt_callback(new KafkaError(BAD_MSG_MSG, BAD_MSG));
//       return;
//     }
//     opt_callback = msg;
//     msg = EMPTY;
//     opt_reply = undefined;
//   }

//   // Need to treat sending buffers different.
//   if (!Buffer.isBuffer(msg)) {
//     var str = msg;
//     if (this.options.json) {
//       if (typeof msg !== 'object') {
//         throw (new NatsError(BAD_JSON_MSG, BAD_JSON));
//       }
//       try {
//         str = JSON.stringify(msg);
//       } catch (e) {
//         throw (new NatsError(BAD_JSON_MSG, BAD_JSON));
//       }
//     }
//     this.sendCommand(psub + Buffer.byteLength(str) + CR_LF + str + CR_LF);
//   } else {
//     var b = new Buffer(psub.length + msg.length + (2 * CR_LF_LEN) + msg.length.toString().length);
//     var len = b.write(psub + msg.length + CR_LF);
//     msg.copy(b, len);
//     b.write(CR_LF, len + msg.length);
//     this.sendCommand(b);
//   }

//   if (opt_callback !== undefined) {
//     this.flush(opt_callback);
//   } else if (this.closed) {
//     throw (new NatsError(CONN_CLOSED_MSG, CONN_CLOSED));
//   }
// }