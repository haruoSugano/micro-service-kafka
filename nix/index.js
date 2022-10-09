const express = require("express");
const kafka = require("kafka-node");
const app = express();
const { Sequelize } = require("sequelize");
const sequelize = require("sequelize");

app.use(express.json());

const dbsAreaRunning = async () => {
  const db = new Sequelize(process.env.POSTGRES_URL);
  const User = db.define("user", {
    name: sequelize.STRING,
    email: sequelize.STRING,
    password: sequelize.STRING,
  });

  db.sync({ force: true });

  const client = new kafka.KafkaClient({
    kafkaHost: process.env.KAFKA_BOOTSTRAP_SERVERS,
  });
  const producer = new kafka.Producer(client);

  producer.on("ready", async () => {
    console.log("producer ready");
    app.post("/", async (req, res) => {
      producer.send(
        [
          {
            topic: process.env.KAFKA_TOPIC,
            messages: JSON.stringify(req.body),
          },
        ],
        async (err, data) => {
          if (err) console.log(err);
          else {
            await User.create(req.body)
            res.send(res.body)
          }
        }
      );
    });
  });
};

setTimeout(dbsAreaRunning, 10000);

app.listen(process.env.PORT);
