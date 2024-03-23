const express = require('express');
const bodyParser = require('body-parser');
const eye = require('eyereasoner');
const fs = require('fs');

const app = express();
const port = 3000;

// Configurazione del middleware per il parsing del corpo della richiesta
app.use(bodyParser.text({ type: 'text/plain', limit: '10mb' }));

// Definizione dell'endpoint per il ragionamento
// Accetta un file .n3 con dati e regole, ritorna un file .n3 con le triple inferite
app.post('/reasoner', (req, res) => {
    // Salvataggio del contenuto del file N3 inviato nella richiesta
    const n3Data = req.body;
    //fs.writeFileSync('input.n3', n3Data);
    
    let ts = Date.now();
    console.log(ts, ":", "START REASONING");

    eye
    .n3reasoner(n3Data, undefined, {
      output: "derivations",
      outputType: "string",
    })
    .then((inferred) => {
      console.log(ts, ":", "END REASONING : ", Math.abs(Math.floor((ts - Date.now()) / 1000)), " sec. ",inferred.length, " bytes");
      // Invio delle triple inferite come risposta
      res.send(inferred);
    })
    .catch(error => {
      console.error("Errore nel ragionamento:", error);
      res.status(500).send("Errore nel ragionamento");
    });

});

// Avvio del server
app.listen(port, () => {
    console.log(`Semantic Server listening at http://localhost:${port}`);
});
