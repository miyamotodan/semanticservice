const express = require('express');
const bodyParser = require('body-parser');
const N3 = require('n3');
const fs = require('fs');
const cors = require("cors");
const { Worker } = require('worker_threads')

const app = express();
const port = 3000;

// Configurazione del middleware per il parsing del corpo della richiesta
app.use(bodyParser.text({ type: 'text/plain', limit: '100mb' }));
app.use(bodyParser.json({ type: 'application/json', limit: '100mb' }));
app.use(cors());

// Avvio del server
app.listen(port, () => {
  console.log(`Semantic Server wk listening at http://localhost:${port}`);
});


// Definizione dell'endpoint per la query
// Accetta q:query s:surces (comunica) c:chunk (se a blocchi)
app.post('/sparql', (req, res) => {
  const body = req.body;
  
  let ts = Date.now();
  console.log(ts, ":", "START QUERYING");
  
  console.log(body);

  const worker = new Worker("./comunica-wk.js", {workerData: {query:body.query, sources:body.sources, chunk:body.chunk}} );

  worker.once("message", result => {
    console.log(ts, ":", "END QUERYING : ", Math.abs(Math.floor((ts - Date.now()) / 1000)), " sec. ",result.length, " bytes");
    // Invio delle triple interrogate come risposta
    res.send({result:result});
  });

  worker.on("error", error => {
    console.error("Errore nella query:", error);
    res.status(500).send("Errore nella query");
  });

  worker.on("exit", exitCode => {
      console.log(`sparql worker exited with code ${exitCode}`);
  })

});







/**
 * 
 * REASONER
 * 
 */


app.post('/reasoner2', async (req, res) => {
  const n3Data = req.body.n3;

  let ts = Date.now();
  console.log(ts, ":", "START REASONING");

  const worker = new Worker("./eye-wk.js", {workerData: {mode:2, data:n3Data}} );

  worker.once("message", inferred => {
    console.log(ts, ":", "END REASONING : ", Math.abs(Math.floor((ts - Date.now()) / 1000)), " sec. ", inferred.length , " bytes");
    // Invio delle triple interrogate come risposta
    
    let inferredQuads = [];
    const parser = new N3.Parser();
    parser.parse(inferred, (error, quad, prefixes) => {
      if (quad)
        inferredQuads.push(quad);
      else
        res.send(inferredQuads);
    });

  });

  worker.on("error", error => {
    console.error("Errore nell'inferenza:", error);
    res.status(500).send("Errore nel'inferenza");
  });

  worker.on("exit", exitCode => {
      console.log(`reasoner worker exited with code ${exitCode}`);
  })
 
});

// Definizione dell'endpoint per il ragionamento
// Accetta un file .n3 con dati e regole, ritorna un file .n3 con le triple inferite
app.post('/reasoner', (req, res) => {
    // Salvataggio del contenuto del file N3 inviato nella richiesta
    const n3Data = req.body.n3;
    //fs.writeFileSync('input.n3', n3Data);
    
    let ts = Date.now();
    console.log(ts, ":", "START REASONING");

    const worker = new Worker("./eye-wk.js", {workerData: {mode:1, data:n3Data}} );

    worker.once("message", inferred => {
      console.log(ts, ":", "END REASONING : ", Math.abs(Math.floor((ts - Date.now()) / 1000)), " sec. ", inferred.length ," bytes");
      // Invio delle triple interrogate come risposta
      
      let inferredQuads = [];
      const parser = new N3.Parser();
      parser.parse(inferred, (error, quad, prefixes) => {
        if (quad)
          inferredQuads.push(quad);
        else
          res.send(inferredQuads);
      });

    });

    worker.on("error", error => {
      console.error("Errore nell'inferenza:", error);
      res.status(500).send("Errore nel'inferenza");
    });
  
    worker.on("exit", exitCode => {
        console.log(`reasoner worker exited with code ${exitCode}`);
    });

});
