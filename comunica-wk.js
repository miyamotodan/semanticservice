const Comunica = require('@comunica/query-sparql');
const N3 = require('n3');
const wt = require("worker_threads");
//console.log("wt", wt.threadId, wt.workerData);

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
            .on('end', () => {  console.log("nn", nn); resolve({nn:nn, n3triples:n3triples}); } );
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
                resolve({nn:nn, n3triples:n3triples});
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
          result += r.n3triples;
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
        result = (await bindingStreamToTriples(bindingStream)).n3triples;
      }
    } else if (q.toLowerCase().indexOf("construct")!=-1) { //TODO:si può fare la gestione a blocchi per CONSTRUCT? verificare
      console.log("--CONSTRUCT--");
        
      let quadStream = await new Comunica.QueryEngine().queryQuads(q, { sources: ep });
      result = (await quadStreamToTriples(quadStream)).n3triples;
    } else {
      console.log("query sparql non riconosciuta (SELECT / CONSTRUCT)");
    }
    console.log("result:",result);
    return result;
  }

  executeQuery(wt.workerData.query,wt.workerData.sources,wt.workerData.cunk).then( response => wt.parentPort.postMessage(response));