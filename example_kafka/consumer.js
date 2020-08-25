const kafka = require("kafka-node");

const client = new kafka.KafkaClient("localhost:2181");

const topics = [
  {
    topic: "kafka-node-dev",
  },
];
const options = {
  autoCommit: false,
};

const consumer = new kafka.Consumer(client, topics, options);

consumer.on("message", function (message) {
  console.log(message);
});
