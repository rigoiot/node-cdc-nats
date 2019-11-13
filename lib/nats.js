/* jslint node: true */
"use strict";

var NATS = require("nats"),
  util = require("util"),
  events = require("events"),
  MQTTPattern = require("mqtt-pattern"),
  uuid = require("uuid");

// Consts
var VERSION = "1.0.0";

// Errors
var BAD_SUBJECT = "BAD_SUBJECT",
  BAD_SUBJECT_MSG = "Subject must be supplied",
  BAD_MSG = "BAD_MSG",
  BAD_MSG_MSG = "Message can't be a function",
  CONN_CLOSED = "CONN_CLOSED",
  CONN_CLOSED_MSG = "Connection closed",
  BAD_JSON = "BAD_JSON",
  BAD_JSON_MSG = "Message should be a JSON object",
  INVALID_ENCODING = "INVALID_ENCODING";

var options = {
  natsURL: "127.0.0.1:8082" // connect directly to nats broker (instantiates a NatsClient)
};

function NatsError(message, code, chainedError) {
  Error.captureStackTrace(this, this.constructor);
  this.name = this.constructor.name;
  this.message = message;
  this.code = code;
  this.chainedError = chainedError;
}

util.inherits(NatsError, Error);
exports.NatsError = NatsError;

// Error codes
exports.BAD_SUBJECT = BAD_SUBJECT;
exports.BAD_MSG = BAD_MSG;
exports.CONN_CLOSED = CONN_CLOSED;
exports.BAD_JSON = BAD_JSON;

exports.version = VERSION;

// Define class Nats
function Nats(opts, topic) {
  var client = this;
  events.EventEmitter.call(this);
  this.ssid = -1;
  this.subs = {};
  this.topic = topic;

  var options = opts;
  Object.assign(options, { json: true });
  this.nc = NATS.connect(options);

  this.nc.on("error", function(error) {
    client.processErr(error);
  });

  this.nc.subscribe(topic || "cdc.client", function(msg, reply, subject, sid) {
    client.processMsg(msg);
  });
}

// Close the connection to the server.
Nats.prototype.close = function() {
  this.nc.close();
  this.closed = true;
  this.removeAllListeners();
  this.ssid = -1;
  this.subs = null;
};

exports.connect = function(opts, topic) {
  return new Nats(opts, topic);
};

util.inherits(Nats, events.EventEmitter);

Nats.prototype.processErr = function(error) {
  this.emit("error", new NatsError(error.message));
};

Nats.prototype.processMsg = function(message) {
  // notify all subs which subject math the message channel
  for (var sid in this.subs) {
    var sub = this.subs[sid];
    if (!MQTTPattern.matches(sub.subject, message.Channel)) {
      continue;
    }
    sub.received += 1;
    // Check for auto-unsubscribe
    if (sub.max !== undefined) {
      if (sub.received === sub.max) {
        delete this.subs[sid];
        this.emit("unsubscribe", sid, sub.subject);
      } else if (sub.received > sub.max) {
        this.unsubscribe(sid);
        sub.callback = null;
      }
    }

    if (sub.callback) {
      sub.callback(message.Data, sub.subject, sid);
    }
  }
};

// Subscribe to a given subject, with optional options and callback.
Nats.prototype.subscribe = function(subject, opts, callback) {
  if (this.closed) {
    throw new NatsError(CONN_CLOSED_MSG, CONN_CLOSED);
  }

  var max;
  if (typeof opts === "function") {
    callback = opts;
    opts = undefined;
  } else if (opts && typeof opts === "object") {
    // FIXME, check exists, error otherwise..
    max = opts.max;
  }

  this.ssid += 1;
  this.subs[this.ssid] = {
    subject: subject,
    callback: callback,
    received: 0
  };

  this.emit("subscribe", this.ssid, subject, opts);

  if (max) {
    this.unsubscribe(this.ssid, max);
  }

  return this.ssid;
};

// Unsubscribe to a given Subscriber Id, with optional max parameter.
Nats.prototype.unsubscribe = function(sid, opt_max) {
  if (sid < 0 || sid == undefined || this.closed) {
    return;
  }

  var sub = this.subs[sid];
  if (sub === undefined) {
    return;
  }
  sub.max = opt_max;
  if (sub.max === undefined || sub.received >= sub.max) {
    delete this.subs[sid];
    this.emit("unsubscribe", sid, sub.subject);
  }
};

Nats.prototype.publish = function(subject, msg, opt_callback) {
  if (this.closed) {
    throw new NatsError(CONN_CLOSED_MSG, CONN_CLOSED);
  }
  if (typeof subject !== "string") {
    throw new NatsError(BAD_SUBJECT_MSG, BAD_SUBJECT);
  }
  if (!msg) {
    new NatsError(BAD_MSG_MSG, BAD_MSG);
  }

  this.nc.publish(this.topic, msg, function(error) {
    if (opt_callback) {
      opt_callback(error);
    }
  });
};

Nats.prototype.unsubscribeRpc = function(sids, timer) {
  let nats = this;
  if (timer != null) {
    clearTimeout(timer);
    timer = null;
  }
  sids.forEach(function(sid) {
    nats.unsubscribe(sid);
  });
};

Nats.prototype.rpc = function(reqSub, resSubs, data, timeout, resProcess) {
  var nats = this;
  return new Promise((resolve, reject) => {
    var sids = [],
      timer = null;

    // console.log('rpc:\n\treq: ', reqSub, '\n\tres: ', resSubs, '\n\ttimeout: ', timeout);

    if (!reqSub || !resSubs) {
      reject({
        error: "subject error"
      });
    }

    if (typeof resSubs === "string") {
      resSubs = [resSubs];
    }

    try {
      for (var i in resSubs) {
        const sid = nats.subscribe(resSubs[i], function(msg) {
          nats.unsubscribeRpc(sids, timer);
          if (resProcess) {
            resolve(resProcess(msg));
          } else {
            resolve(msg);
          }
        });
        sids.push(sid);
      }

      // Publish request
      nats.publish(reqSub, data, error => {
        if (error !== null) {
          console.log("rpc: publish error: ", error);
          nats.unsubscribeRpc(sids, timer);
          reject({
            error: error
          });
        }
      });

      timeout = timeout || 10;
      timer = setTimeout(() => {
        timer = null;
        nats.unsubscribeRpc(sids, timer);
        console.log("rpc: request time out");
        resolve({
          code: 408,
          message: "request time out"
        });
      }, timeout * 1000);
    } catch (err) {
      console.log("rpc: error: ", err);
      nats.unsubscribeRpc(sids, timer);
      reject({
        error: err
      });
    }
  });
};
