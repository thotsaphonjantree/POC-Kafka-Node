const express = require("express");
const kafka = require("kafka-node");
const EventEmitter = require("events");
/////////////////////////////////////
const myEmitter = new EventEmitter();
/////////////////////////////////////
const client = new kafka.KafkaClient("localhost:2181");
/////////////////////////////////////
const Producer = kafka.Producer;
const producer = new Producer(client);

const app = express();

app.get("/", (req, res) => {
  let random = Math.random();
  send(JSON.stringify({ name: "John", id: random }));
  myEmitter.on(`response_${random}`, (data) => {
    res.send(data.value);
  });
});

startServer(app);

const topics = [
  {
    topic: "reply",
  },
];
const options = {
  autoCommit: true,
};
const consumer = new kafka.Consumer(client, topics, options);

consumer.on("message", function (message) {
  let id = JSON.parse(message.value).id;
  myEmitter.emit(`response_${id}`, message);
});

function startServer(app) {
  const PORT = 8080;
  const HOST = "0.0.0.0";
  app.listen(8080);
  console.log(`Running on http://${HOST}:${PORT}`);
}

function send(sentMessage) {
  const payloads = [
    { topic: "kafka-node-dev", messages: sentMessage, partition: 0 },
  ];

  producer.send(payloads, function (err, data) {
    console.log("send data ", sentMessage);
  });
}
