const { publish, closeOnErr } = require("./producer");

var amqpConnection = null;

// A worker that acks messages only if processed succesfully
const startWorker = (amqpConn) => {
  amqpConnection = amqpConn;
  amqpConn.createChannel(function (err, ch) {
    if (closeOnErr(err)) return;
    ch.on("error", function (err) {
      console.error("[AMQP] channel error", err.message);
    });
    ch.on("close", function () {
      console.log("[AMQP] channel closed");
    });
    ch.prefetch(10);
    ch.assertQueue("jobs", { durable: true }, function (err, _ok) {
      if (closeOnErr(err)) return;
      ch.consume(
        "jobs",
        function processMsg(msg) {
          work(msg, function (ok) {
            try {
              if (ok) ch.ack(msg);
              else ch.reject(msg, true);
            } catch (e) {
              closeOnErr(e);
            }
          });
        },
        { noAck: false }
      );
      console.log("Worker is started");
    });
  });
};

const work = (msg, cb) => {
  console.log("Got msg", msg.content.toString());
  cb(true);
};

setInterval(function () {
  publish("", "jobs", new Buffer.from("work work work"));
}, 500);

module.exports = { startWorker };
