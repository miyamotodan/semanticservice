
const N3 = require('n3');
const eye = require('eyereasoner');
const wt = require("worker_threads");
const Comunica = require('@comunica/query-sparql');
const fs = require('fs');
//console.log("wt", wt.threadId, wt.workerData);

const applyRules2 = async (data) => {
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

  const applyRules = async (data) => {

    let inferred = await eye
    .n3reasoner(data, undefined, {
      output: "derivations",
      outputType: "string",
    });
    
    return inferred;

  }

  //esempio di ragionatore custom che costruisce un grafo facendo sia query sparql che ragionamenti N3
  const applyRulesCustom = async (data) => {

    let inferred = '';
    let store = new N3.Store();
    
    //carico i dati originali nello store
    let tot = await storeLoad(store,data);

    //faccio una query sparql sullo store caricato
    let q = fs.readFileSync("./query/c_query1.sparql").toString();
    console.log("QUERY:",q);
    
    let rs = await executeStoreQuery(q,store);
    rs = getResultSet(rs.bindings);
    //console.log(rs);

    //uso il resultset per creare delle triple inferite
    let i = 0;
    rs.forEach(row => {
      i++;
      inferred += graphNode(i,row.c,row.t,row.c,"conteggio delle triple caricate nello store per la classe <"+row.c+">",row.c,"circle");
    });

    //aggiungo le triple inferite allo store
    tot += await storeLoad(store,inferred);

    let rule = fs.readFileSync("./rules/c_rule1.n3").toString();
    console.log("RULE:",rule);
    
    let inferred2 = await applyRules(data + inferred + rule);
    inferred += inferred2;

    fs.writeFileSync('./log/inferred.n3', inferred);
  
    return inferred;

  }

  //carica nello store delle triple
  async function storeLoad(store, data) {
    const parser = new N3.Parser();
    let i=0;
    return new Promise((resolve, reject) => {
      parser.parse(data, async (error,quad,prefixes) => {
        if (quad) {
          //console.log(quad);
          store.addQuad(quad);
          i++;
        }
        else 
        if (error) {
          console.log("ERROR",error);
          reject(error); 
        } else {
          console.log('parsing done.');
          console.log("storing done, quads:", i);
          resolve(i);
        }
      });
    });
  }

  //interroga uno store 
  const executeStoreQuery = async (query, store) => {
    const myEngine = new Comunica.QueryEngine();
    const bindingsStream = await myEngine.queryBindings(query, {
        sources: [store],
    });

    let nn = 0;
    let results = [];
    return new Promise((resolve, reject) => {
        // Consume results as a stream (best performance)
        bindingsStream
            .on("data", (binding) => {
                //console.log(binding.toString()); // Quick way to print bindings for testing
                nn++;
                results.push(binding);
            })
            .on("end", () => {
                // The data-listener will not be called anymore once we get here.
                resolve({ count: nn, bindings: results });
            })
            .on("error", (error) => {
                console.error(error);
                reject(e);
            });
    });
  };

  //ricava un json sempificato dai bindings
  function getResultSet(vbindings) {
    let rs = [];
    for (let i = 0; i < vbindings.length; i++) {
        let r = {};
        for (const [key, value] of vbindings[i]) {
            //console.log("chiave",key);
            //console.log("valore",value, "literal:",value instanceof N3.Literal);
            let v;
            if (value instanceof N3.Literal) v = value.id.replaceAll('"', "")
            else if (value instanceof N3.NamedNode) v = value.id
            else v = value.value;
            r[key.value] = v;
        }
        rs.push(r); 
    }
    return rs;
  }

  function graphNode(id, term, tooltip, label, descr, url, shape) {
    return `
            <http://view/digraph> <http://view/hasNode>  <http://localhost/nodes/`+id+`>.
            <http://localhost/nodes/`+id+`> <http://view/term> <`+term+`>;
            <http://view/dot/attribute/tooltip> "`+tooltip+`";
            <http://view/dot/attribute/label> "`+label+`";
            <http://view/dot/attribute/descr> "`+descr+`";
            <http://view/dot/attribute/shape> "`+shape+`";
            <http://view/dot/attribute/labelURL> <`+url+`> .
            `;
  }

  function graphEdge(id, tooltip, label, url, shape) {
    return `
            <http://view/digraph> <http://view/hasEdge>  <http://localhost/edges/`+id+`>.
            <http://localhost/edges/`+id+`>. <http://view/dot/attribute/shape> "`+shape+`";
            <http://view/dot/attribute/label> "`+label+`";
            <http://view/dot/attribute/tooltip> "`+tooltip+`";
            <http://view/dot/attribute/labelURL> <`+url+`> ;
            <http://view/source> <`+source+`> ;
            <http://view/target> <`+target+`> .
            `;
  }

  if (wt.workerData.mode==2)
    applyRules2(wt.workerData.data).then(inferred => wt.parentPort.postMessage(inferred));
  else
  if (wt.workerData.mode==1)
    applyRules(wt.workerData.data).then(inferred => wt.parentPort.postMessage(inferred));
  else 
    applyRulesCustom(wt.workerData.data).then(inferred => wt.parentPort.postMessage(inferred));
    