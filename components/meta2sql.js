const N3 = require('n3');
const eye = require('eyereasoner');
const fs = require('fs');
const Graph = require('../components/graph'); 
const Store = require('../components/store');
const squel = require("squel");
var lh = require('lodash'); 

const applyRules = async (data) => {

    let inferred = await eye
    .n3reasoner(data, undefined, {
      output: "derivations",
      outputType: "string",
    });
    
    return inferred;

}

//utilizzando una mappatura variabile --> tabella rimette in ordine i dati che poi vengono usati per costruire le join 
const assignMapping = (data, vars, mapping) => {
    if (mapping.get(vars[0])) //mapping della prima variabile già impostato
        if (mapping.get(vars[0]) != data[0] && mapping.get(vars[0]) == data[1]) { //se l'impostazione non corrisponde ai dati
            //vanno invertite le variabili (relazione 1-n invece di 1-1)
            let t = data[0];
            data[0] = data[1];
            data[1] = t;
            t = data[2];
            data[2] = data[3];
            data[3] = t;
            assignMapping(data, vars, mapping);
        } else //se l'impostazione corrisponde si passa alla seconda variabile
          if (mapping.get(vars[1])) //mapping della seconda variabile già impostato
              if (mapping.get(vars[1]) != data[1]) //se non corrisponde 
                console.log("**errore nel mapping**");
              else {}
            else //mapping della seconda variabile non impostato
              if (![...mapping.values()].includes(data[1])) { //se il dato non è stato impostato su altre variabili
                mapping.set(vars[1], data[1]); 
              } else
                console.log("**errore nel mapping**");

     else //mapping della prima variabile non è impostato
      if (![...mapping.values()].includes(data[0])) { //se il dato non è stato impostato su altre variabili
        mapping.set(vars[0],data[0]);
        assignMapping(data,vars,mapping); 
      } else
         console.log("**errore nel mapping**");
      
      return
  }

//classe che espone un metodo di ragionamento customdedicato alla costruzione di query SQL a partire da metadati 
class Meta2SQL {

    applyRulesCustom = async (data) => {

        let inferred = '';
        let store = new N3.Store();
        
        //carico i dati originali nello store
        let tot = await Store.storeLoad(store,data);
        let rule = fs.readFileSync("./rules/meta2sql_4.n3").toString();
        //console.log("RULE:",rule);
        inferred = await applyRules(data + inferred + rule);
        fs.writeFileSync('./log/inferred.n3', inferred);
        tot += await Store.storeLoad(store,inferred);
    
        //faccio una query sparql sullo store caricato
        let q = fs.readFileSync("./query/meta2sql_4.sparql").toString();
        //console.log("QUERY:",q);
        
        let rs = await Store.executeStoreQuery(q,store);
        rs = Store.getResultSet(rs.bindings);
        console.log(rs);
    
        let gr = lh.groupBy(rs,'r'); //raggruppo le righe del resultSet per regola 
        let vq = [];//vettore query 
        let nq = 0; //numero query
       
        //per ogni regola
        lh.each(gr, (v,k) => {
          console.log("processing:",k);
          let tree = new Graph(); //albero per ordinare le join
          let mapping = new Map(); //mappa le variabili con le tabelle del DB nelle Join
          let nv = 0; //numero vertici dell'albero
          let mainTable = "";
          nq++;
          lh.each(v, (e,i) => {
            let ct = e.l.substring(0,e.l.indexOf("("));
            if (e.o=="1") { //il primo termine della regola individua la tabella principale
              nv = 0;
              let data = e.c.split("|");
              vq[nq]=squel.select().from(data[0]).field(data[1]); //proietto la PK
              mainTable=data[0];
              mapping.set(e.v, data[0]); //mappa la variabile principale (ordine 1)
              tree.addVertex(data[0],data); nv++;
            } else {
              //console.log("ct:",ct );
              if (ct=="ObjectPropertyInstance") { //costruisce le join
                let vars = e.v.split("|");
                let data = e.c.split("|");
                assignMapping(data,vars,mapping);
                tree.addVertex(data[1],data); nv++;
                tree.addEdge(data[0],data[1],true); //arco orientato
                //console.log(data);
              } else
              if (ct=="DataPropertyEqualsTo") {
                  let vars = e.v.split("|");
                  let data = e.c.split("|"); 
                  vq[nq].field(data[0]);
                  if (e.b=="true") vq[nq].where(data[0]+" = '"+vars[1]+"'");
                  else vq[nq].where(data[0]+" <> '"+vars[1]+"'");
              } else
              if (ct=="DataPropertySum") {
      
              } else {
                console.log("*costrutto non riconosciuto*",ct);
              }
              
            }
          });

          //console.log(tree);
          //faccio una visita in ampiezza dell'albero generato delle ObjectPropertyInstance
          let bfs = tree.bfs(mainTable); 
          //console.log("BFS",bfs);
          
          //per ogni nodo (tranne la radice) inserisco una join
          bfs.forEach((e,i) => {
            let data = tree.getData(e);
            //console.log("bfs["+i+"]:"+ data)
            if (data[1] && data[2] && data[3]) vq[nq].join("" + data[1], null ,data[2] + " = " + data[3]);
          });

        })

        //stampo le query
        vq.forEach( (e,i) => console.log(i, e.toString() )) ;    
    
        return inferred;
    
      }

}

const meta2sqlInstance = new Meta2SQL();
module.exports = {
    applyRulesCustom: meta2sqlInstance.applyRulesCustom.bind(meta2sqlInstance)
};