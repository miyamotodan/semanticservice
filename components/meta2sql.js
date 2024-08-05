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

//classe che espone un metodo di ragionamento custom dedicato alla costruzione di query SQL a partire da metadati
// - esamina le relazioni tra le entità e
//    + capisce la drezione delle relazioni (per mettere correttamente nella query PK e FK)
//    + crea un albero delle relazioni e lo esplora con una BFS in modo da fare crretamente le JOIN
// - "capisce" se deve o meno fare una group by (se ci sono aggregazioni)
// - applica le WHERE o le HAVING per le condizioni sulle proprietà 
// - "capisce" se deve fare una subquery
//
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
          //console.log("processing:",k);
          let tree = new Graph(); //albero per ordinare le join
          let mapping = new Map(); //mappa le variabili con le tabelle del DB nelle Join
          let nv = 0; //numero vertici dell'albero
          let mainTable = {};
          let fields2gb = []; //indica in campi su cui fare il group by: tutti quelli non aggegati
          let gb = false;     //indica che bisogna fare il group by
          let mq = false;     //indica che bisogna fare una main-query e una sub query
          let sqd = [];        //vettore che prepara la costruzione di parte della sub-query (che può essere l'unica query) perché alcune decisioni su come farla devono essere prese a valle della raccolta di tutti i dati
          let mqd = [];        //vettore che prepara la costruzione della eventuale query principale che contiene la sub-quer
          nq++;
          lh.each(v, (e,i) => {
            let ct = e.l.substring(0,e.l.indexOf("("));
            if (e.o=="1") { //il primo termine della regola individua la tabella principale
              nv = 0;
              let data = e.c.split("|");
              vq[nq]=squel.select().from(data[0]).field(data[1]); //proietto la PK
              fields2gb.push(data[1]);
              mainTable={table:data[0],pk:data[1].substring(data[1].lastIndexOf('.')+1,100)}; //salvo la PK con il solo nome del campo in prospettiva della subquery eventuale
              mapping.set(e.v, data[0]); //mappa la variabile principale per le join (ordine 1)
              tree.addVertex(data[0],data); nv++; //aggiunge il nodo della tabella all'albero delle join
            } else {
              //console.log("ct:",ct );
              if (ct=="ObjectPropertyInstance") { //costruisce le join
                let vars = e.v.split("|");
                let data = e.c.split("|");
                assignMapping(data,vars,mapping);   //mappa le variabili
                tree.addVertex(data[1],data); nv++; //aggiunge il nodo della tabella all'albero delle join
                tree.addEdge(data[0],data[1],true); //arco orientato
                //console.log(data);
              } else
              if (ct=="DataPropertyEqualsTo") {
                  let vars = e.v.split("|");
                  let data = e.c.split("|"); 
                  vq[nq].field(data[0]);   //metto il campo in proiezione
                  fields2gb.push(data[0]); //lo segno tra quelli che possono andare in GROUP BY
                  sqd.push([e.b, data[0], vars[1]]); //conservo i dati per la scrittura di WHERE/HAVING
              } else
              if (ct=="DataPropertySum") {
                  gb = true;
                  let vars = e.v.split("|");
                  let data = e.c.split("|"); 
                  vq[nq].field("SUM(" + data[0] + ")",vars[1]); //metto il campo in proiezione come SUM
              } else
              if (ct=="DataPropertyListSomeIn") {
                  let vars = e.v.split("|");
                  let data = e.c.split("|");
                  vq[nq].field(data[0], data[0].replaceAll('.',''));    //metto il campo in proiezione
                  mq = true;                                            //si deve fare una query annidata
                  let el = vars.slice(1).map((e) => "'"+e+"'").join(",");
                  mqd.push([e.b, data[0].replaceAll('.',''), el, "="]); //conservo i dati per la scrittura delle condizioni HAVING sulla quey annidata
              } else
              if (ct=="DataPropertyListAllNotIn") {
                  let vars = e.v.split("|");
                  let data = e.c.split("|");
                  vq[nq].field(data[0], data[0].replaceAll('.',''));    //metto il campo in proiezione
                  mq = true;                                            //si deve fare una query annidata
                  let el = vars.slice(1).map((e) => "'"+e+"'").join(",");
                  mqd.push([e.b, data[0].replaceAll('.',''), el, ">"]); //conservo i dati per la scrittura delle condizioni HAVING sulla quey annidata
              } else
              if (ct=="ClassInstance") {
                //non deve fare nulla
              } else {
                console.log("*costrutto non riconosciuto*",ct);
              }
              
            }
          });

          //console.log(tree);
          //faccio una visita in ampiezza dell'albero generato delle ObjectPropertyInstance
          let bfs = tree.bfs(mainTable.table); 
          //console.log("BFS",bfs);
          
          //per ogni nodo (tranne la radice) inserisco una join
          bfs.forEach((e,i) => {
            let data = tree.getData(e);
            //console.log("bfs["+i+"]:"+ data)
            if (data[1] && data[2] && data[3]) vq[nq].join("" + data[1], null ,data[2] + " = " + data[3]);
          });

          console.log("*sqd*",sqd);

          //qui devo capire quando fare una WHERE e quando una HAVING
          sqd.forEach((e,i) => {
            //console.log("("+i+") ",e);
            if (!e[2].startsWith("BR_var")) e[2]="'"+e[2]+"'"; //se è una variabile o un valore
            if (gb)
              if (fields2gb.includes(e[1]))
                if (e[0]=="true") vq[nq].having(e[1]+" = "+e[2]);
                else vq[nq].having(e[1]+" <> "+e[2]);
              else {}  
            else   
              if (e[0]=="true") vq[nq].where(e[1]+" = "+e[2]);
              else vq[nq].where(e[1]+" <> "+e[2]);
          });
          
          //group by su tutti i campi proiettati tranne quelli aggregato
          if (gb) vq[nq].group(fields2gb.join(','));

          console.log("*mqd*",mqd);

          //query annidata (1 livello...)
          if (mq) 
            vq[nq] = squel.select().from(vq[nq]).field(mainTable.pk); 
            vq[nq].group(mainTable.pk);
            let sqlexpr = squel.expr();
            mqd.forEach((e,i) => {
              sqlexpr.or("SUM(CASE WHEN "+e[1]+" IN ("+e[2]+") THEN 1 END) "+e[3]+" 0");
            });
            vq[nq].having(sqlexpr);
            
        })

        //stampo le query
        vq.forEach( (e,i) => console.log("\n["+i+"] ", e.toString() )) ;    
    
        return inferred;
    
      }

}

const meta2sqlInstance = new Meta2SQL();
module.exports = {
    applyRulesCustom: meta2sqlInstance.applyRulesCustom.bind(meta2sqlInstance)
};