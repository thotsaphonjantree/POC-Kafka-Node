const kafka = require("kafka-node");

const client = new kafka.KafkaClient("localhost:2181");

const Producer = kafka.Producer;
const producer = new Producer(client);

producer.on("ready", function () {
  console.log("Producer is ready");
  producer.createTopics(
    [
      { topic: "kafka-node-dev", partitions: 1, replicationFactor: 1 },
      { topic: "reply", partitions: 1, replicationFactor: 1 },
    ],
    (result, error) => {}
  );
});

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
  send(message.value);
});

function send(sentMessage) {
  payloads = [{ topic: "reply", messages: sentMessage, partition: 0 }];
  producer.send(payloads, function (err, data) {
    console.log("send data ", sentMessage);
  });
}
