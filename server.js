const express = require('express');
const bodyParser = require('body-parser');
const eye = require('eyereasoner');
const Comunica = require('@comunica/query-sparql');
const N3 = require('n3');
const fs = require('fs');
const cors = require("cors");

const app = express();
const port = 3000;

// Configurazione del middleware per il parsing del corpo della richiesta
app.use(bodyParser.text({ type: 'text/plain', limit: '100mb' }));
app.use(bodyParser.json({ type: 'application/json', limit: '100mb' }));
app.use(cors());

const applyRules = async (data) => {
  const chunks = [];
  const Module = await eye.SwiplEye({
    print: (x) => {
      chunks.push(x);
    },
    arguments: ["-q"],
  });
  // Load the the strings data and query as files data.n3 and query.n3 into the module
  Module.FS.writeFile("data.n3", data);
  // Execute main(['--nope', '--quiet', './data.n3', '--query', './query.n3']).
  eye.queryOnce(Module, "main", [
    "--nope",
    "--quiet",
    "--pass-only-new",
    "./data.n3",
  ]);
  return chunks.join("\n");
};
app.post('/reasoner2', async (req, res) => {
  const n3Data = req.body.n3;

  let ts = Date.now();
  console.log(ts, ":", "START REASONING");
  let inferred = await applyRules(n3Data);
  console.log(ts, ":", "END REASONING : ", Math.abs(Math.floor((ts - Date.now()) / 1000)), " sec. ",inferred.length, " bytes");

  //console.log("inferred",inferred);

  let inferredQuads = [];
  const parser = new N3.Parser();
  parser.parse(inferred, (error, quad, prefixes) => {
    if (quad)
      inferredQuads.push(quad);
    else
      res.send(inferredQuads);
  });
});


// Definizione dell'endpoint per il ragionamento
// Accetta un file .n3 con dati e regole, ritorna un file .n3 con le triple inferite
app.post('/reasoner', (req, res) => {
    // Salvataggio del contenuto del file N3 inviato nella richiesta
    const n3Data = req.body.n3;
    //fs.writeFileSync('input.n3', n3Data);
    
    let ts = Date.now();
    console.log(ts, ":", "START REASONING");

    eye
    .n3reasoner(n3Data, undefined, {
      output: "derivations",
      outputType: "quads",
    })
    .then((inferred) => {
      console.log(ts, ":", "END REASONING : ", Math.abs(Math.floor((ts - Date.now()) / 1000)), " sec. ",inferred.length, " bytes");
      // Invio delle triple inferite come risposta
      //console.log(inferred);
      res.send(inferred);
    })
    .catch(error => {
      console.error("Errore nel ragionamento:", error);
      res.status(500).send("Errore nel ragionamento");
    });

});

// Definizione dell'endpoint per la query
// Accetta q:query s:surces (comunica) c:chunk (se a blocchi)
app.post('/sparql', (req, res) => {
  const body = req.body;
  
  let ts = Date.now();
  console.log(ts, ":", "START QUERYING");
  
  //console.log(body);

  executeQuery(body.query,body.sources,body.cunk)
  .then((result) => {
    console.log(ts, ":", "END QUERYING : ", Math.abs(Math.floor((ts - Date.now()) / 1000)), " sec. ",result.length, " bytes");
    // Invio delle triple interrogate come risposta
    res.send({result:result});
  })
  .catch(error => {
    console.error("Errore nella query:", error);
    res.status(500).send("Errore nella query");
  });

});

 //costruisce un termine di una tripla ne caso di lettura dei datida query SELECT
 const processTerm = (tvalue, ttype, tlanguage) => {
  let res = "";
  if (ttype == "NamedNode") res = "<" + tvalue.replaceAll(' ','') + ">"; //per problemi di eventuali uri con spazi
  else if (ttype == "BlankNode") {
    if (!tvalue.startsWith("nodeID:")) res = "_:" + tvalue;
    else res = "<" + tvalue + ">";
  } 
  else
    res =
      '"' +
      tvalue.replaceAll('"', "'").replaceAll('\n',' ') + //per problemi di eventuali stringhe con apici doppi e invio
      '"' +
      (tlanguage != "" ? "@" + tlanguage : "");
  return res;
};

const bindingStreamToTriples = (bindingsStream) => {
  let n3triples = "";
  let nn=0;
  return new Promise((resolve, reject) => {
    bindingsStream
          .on('data', binding => {
              nn++;
              // Each variable binding is an RDFJS term
              let s = binding.get("s").value;
              let st = binding.get("s").termType;
              //console.log(s, st);

              let p = binding.get("p").value;
              let pt = binding.get("p").termType;
              //console.log(p, pt);

              let o = binding.get("o").value;
              let ot = binding.get("o").termType;
              let ol = binding.get("o").language;
              //console.log(o, ot, ol);

              if (n3triples == "")
                n3triples += processTerm(s, st, undefined) + " " + processTerm(p, pt, undefined) + " " + processTerm(o, ot, ol) + " .";
              else
                n3triples += "\n" + processTerm(s, st, undefined) + " " + processTerm(p, pt, undefined) + " " + processTerm(o, ot, ol) + " .";
          } )
          .on('error', e => { 
           
            console.log('ERROR : bindingStreamToTriples'); 
            reject(e) 
          })
          .on('end', () => {  console.log("nn", nn); resolve({count:nn, triples:n3triples}); } )
  })
}

const quadStreamToTriples = (quadStream) => {
  let n3triples = "";
  let nn=0;
  const writer = new N3.Writer({ prefixes: {} });
  return new Promise((resolve, reject) => {
    quadStream
          .on('data', quad => {
              nn++;
              writer.addQuad(quad);   
          })
          .on('error', e => {
            
            console.log('ERROR : quadStreamToTriples'); 
            reject(e) 
          })
          .on('end', () => {  
            console.log("nn", nn); 
            writer.end((error, n3triples) => {
              resolve(n3triples);
            });
          } )
  })
}

//esegue la query sulla sorgente e ritorna le triple
const executeQuery = async (q, ep, c) => {
  let result="";
  if (q.toLowerCase().indexOf("select")!=-1) {
    console.log("--SELECT--");
    
    if (c) { //la query va fatta a blocchi con limit e offset
      let next=true;
      let start = 0;
      while (next) {
        let qc = q + " LIMIT "+c+" OFFSET "+start;
        console.log(qc);
        //let bindingStream = await new Comunica.QueryEngine().queryBindings(qc, { sources: ep, forceHttpGet:true }); 
        let bindingStream = await new Comunica.QueryEngine().queryBindings(qc, { sources: ep }); 
        let r = await bindingStreamToTriples(bindingStream);
        result += r.triples;
        if (r.count==c) { result +="\n"; start += c; }
        else next = false; 
      }
    } else {

      //++++ creazione di un motore con configurazione ad-hoc per provare ad usare "forceHttpGet" ma non si capisce come (https://github.com/comunica/comunica/discussions/1315)
      //const QueryEngineFactory = require('@comunica/query-sparql').QueryEngineFactory;
      //const myEngine = await new QueryEngineFactory().create({ configPath: 'config-default.json' });
      //let bindingStream = await myEngine.queryBindings(q, { sources: ep });
      //+++
      
      let bindingStream = await new Comunica.QueryEngine().queryBindings(q, { sources: ep });
      result = (await bindingStreamToTriples(bindingStream)).triples;
    }
  } else if (q.toLowerCase().indexOf("construct")!=-1) { //TODO:si può fare la gestione a blocchi per CONSTRUCT? verificare
    console.log("--CONSTRUCT--");
      
    let quadStream = await new Comunica.QueryEngine().queryQuads(q, { sources: ep });
    result = await quadStreamToTriples(quadStream);
  } else {
    console.log("query sparql non riconosciuta (SELECT / CONSTRUCT)");
  }
  console.log("result length:",result.length);
  return result;
}

// Avvio del server
app.listen(port, () => {
    console.log(`Semantic Server listening at http://localhost:${port}`);
});
