const express = require('express')
var bodyParser = require('body-parser')
const app = express();
require('dotenv').config();
var path = require('path');
const cors = require('cors');
const swaggerUI = require("swagger-ui-express");
const swaggerJsDoc = require("swagger-jsdoc");

// parse application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: false }))
// parse application/json
app.use(bodyParser.json())

const port = process.env.PORT || 4009;
var routeBusController = require('./controller/busController.js');

const options = {
  definition: {
    openapi: "3.0.0",
    info: {
      title: "Azure Bus Manager App",
      version: "1.0.0",
      description: "Documentation for Azure Bus Manager App"
    },
    servers: [
      {
        url: "http://localhost:4009/"
      }
    ]
  },
  apis: ["./controller/*.js"]
};

const specs = swaggerJsDoc(options);

app.use("/api-docs", swaggerUI.serve, swaggerUI.setup(specs));

app.get('/', (req, res) => {
  return res.json({ Message: 'Azure Bus Manager App' })
});
app.use('/bus', routeBusController);
app.use(cors());

app.listen(port, () => {
  console.log(`App listening on port ${port}`)
});

module.exports = app;