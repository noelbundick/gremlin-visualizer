const express = require('express');
const bodyParser = require('body-parser');
const gremlin = require('gremlin');
const cors = require('cors');
const app = express();
const port = 3001;

const cosmos = {
  endpoint: 'wss://<COSMOSDB_INSTANCE>.gremlin.cosmos.azure.com:443/',
  // for database: "main", collection: "mygraph"
  collection: '/dbs/main/colls/mygraph',
  key: "<ACCESS_KEY>",
};

app.use(cors({
  credentials: true,
}));

// parse application/json
app.use(bodyParser.json());

function mapToObj(inputMap) {
  let obj = {};

  for (const prop in inputMap) {
    obj[prop] = inputMap[prop][0];
  }

  return obj;
}

function edgesToJson(edgeList) {
  return edgeList.map((edge) => ({
    id: typeof edge.id !== "string" ? JSON.stringify(edge.id) : edge.id,
    from: edge.from,
    to: edge.to,
    label: edge.label,
    properties: mapToObj(edge.properties),
  }));
}

function nodesToJson(nodeList) {
  return nodeList.map((node) => ({
    id: node.id,
    label: node.label,
    properties: mapToObj(node.properties),
    edges: edgesToJson(node.edges),
  }));
}

function makeQuery(query, nodeLimit) {
  const nodeLimitQuery = !isNaN(nodeLimit) && Number(nodeLimit) > 0 ? `.limit(${nodeLimit})` : "";
  return `${query}${nodeLimitQuery}.dedup().as('node').project('id', 'label', 'properties', 'edges').by(__.id()).by(__.label()).by(__.valueMap()).by(__.outE().project('id', 'from', 'to', 'label', 'properties').by(__.id()).by(__.select('node').id()).by(__.inV().id()).by(__.label()).by(__.valueMap()).fold())`;
}

app.post('/query', (req, res, next) => {
  const nodeLimit = req.body.nodeLimit;
  const query = req.body.query;

  const authenticator = new gremlin.driver.auth.PlainTextSaslAuthenticator(
    cosmos.collection,
    cosmos.key
  );
  const client = new gremlin.driver.Client(
    cosmos.endpoint,
    {
      authenticator,
      traversalsource: "g",
      rejectUnauthorized: true,
      mimeType: "application/vnd.gremlin-v2.0+json",
    }
  );

  client.submit(makeQuery(query, nodeLimit), {})
    .then((result) => res.send(nodesToJson(result._items)))
    .catch((err) => next(err));

});

app.listen(port, () => console.log(`Simple gremlin-proxy server listening on port ${port}!`));