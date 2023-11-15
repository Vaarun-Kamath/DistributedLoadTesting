const http = require('http');
const express = require('express');

const app = express();
const PORT = 5000;

var num_requests = 0;
var num_responses = 0;
var a = 0;
app.get("/ping",  (req, res) => {
  const entry = process.hrtime()[1];
  num_requests += 1;
  a++;
  res.send(JSON.stringify({"status": 200, "value": a, "entry": entry, "exit": process.hrtime()[1]}));
  num_responses += 1;
})

app.get("/metrics", (req, res) => {
  res.send("Num of requests: " + num_requests + "\nNum responses: " + num_responses)
})

app.listen(PORT, ()=>{
  console.log(`Load Testing Server started on port: ${PORT}`);
})
