const N3 = require('n3');
const eye = require('eyereasoner');
const fs = require('fs');
const Graph = require('../components/graph'); 
const Store = require('../components/store');
var lh = require('lodash');
const knex = require('knex')({ client: 'pg', wrapIdentifier: (value, origImpl) => value }); 

const applyRules = async (data) => {

    let inferred = await eye
    .n3reasoner(data, undefined, {
      output: "derivations",
      outputType: "string",
    });
    
    return inferred;

}

const assignFieldMapping = (data, vars, fieldMapping)=> {

    //console.log("++++data",data);
    //console.log("++++vars",vars);
    //verifico se il campo è già in proiezione
    let fn = fieldName(data[0])+"_"+vars[0];
    let prj=true;
    //if (fieldMapping.get(data[0]+"_"+vars[0]))
    if (fieldMapping.get(data[0]))   
      //variaile già proiettata 
      prj=false;
    else 
      //fieldMapping.set(data[0]+"_"+vars[0],fn);
      fieldMapping.set(data[0],fn);

    let res = {prj:prj,alias:fn,field:data[0]};
    //console.log("+++res",res);

    return res
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
                console.log("**errore nel mapping** ",mapping.get(vars[1])+"<>"+data[1]);
              else {}
            else //mapping della seconda variabile non impostato
              if (![...mapping.values()].includes(data[1])) { //se il dato non è stato impostato su altre variabili
                mapping.set(vars[1], data[1]); 
              } else {
                console.log("++alert nel mapping++", data[1],vars[1]); //serve un alias 

                //conto quante variabili che mappano la stessa tabella, se le variabili sono più di una quindi c'è un conteggio in result
                //allora aggiungo nel mapping vicino al nome della tabella anche il conteggio in modo da poter costruire l'alias
                let values = Array.from(mapping.values());
                let result = lh.countBy(values);
                //console.log("COUNTS", result);
                if (result[data[1]]) 
                  mapping.set(vars[1], data[1]+"|"+result[data[1]]);  
                else 
                  mapping.set(vars[1], data[1]); 
              }
                

     else //mapping della prima variabile non è impostato
      if (![...mapping.values()].includes(data[0])) { //se il dato non è stato impostato su altre variabili
        mapping.set(vars[0],data[0]);
        assignMapping(data,vars,mapping); 
      } else
         console.log("**errore nel mapping**");
      
      return
  }
  
  //estrae il nome del campo
  function fieldName (f) {
    let res = f;
    if (f.indexOf('.')!=-1)  res=  f.substring(f.lastIndexOf('.')+1);
    return res;
  }

  //estrae il nome della tabella
  function tabName (f) {
    let res = f;
    if (f.indexOf('.')!=-1)  res= f.substring(0,f.lastIndexOf('.'));
    return res;
  }

  function aliasTabName (e,data,tabMapping) {
    //console.log("aliasTabName-->", e, data, tabMapping);
    let res = null;
    let mapp = tabMapping.get(e).split('|'); //se esiste un valore vicino alla tabella nel mapping deve essere usato per l'alias 
    console.log("(",mapp,")");
    if (mapp.length>1) res=data[1]+"_"+mapp[1]; //se esiste un valore vicino alla tabella nel mapping deve essere usato per l'alias
    return res;
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
        //console.log(rs);
    
        let gr = lh.groupBy(rs,'r'); //raggruppo le righe del resultSet per regola 
        let vq = [];//vettore query 
        let nq = 0; //numero query
        console.log(gr);

        //per ogni regola
        lh.each(gr, (v,k) => {
          console.log(">> processing:",k);
          let tree = new Graph(); //albero per ordinare le join
          
          let tabMapping = new Map(); //mappa le variabili con le tabelle del DB nelle Join
          let fieldMapping = new Map(); //mappa i campi da proiettare con gli alias
          let varMapping = new Map(); //mappa le variabili con gli alias

          let nv = 0; //numero vertici dell'albero
          let mainTable = {};
          let fields2gb = []; //indica in campi su cui fare il group by: tutti quelli non aggegati
          let gb = false;     //indica che bisogna fare il group by
          let hwc = [];       //vettore che prepara la costruzione della HAVING o della WHERE
          let scw = [];       //vettore che prepara la costruzione della sum case when

          nq++;
          vq[nq]={};
                  
          //ordino i costrutti della regola ATTENZIONE ALTRIMENTI IL CENSIMENTO DELLE VARIABILI E DELLE TABELLE POTREBBE FALLIRE
          v=lh.orderBy(
            v, 
            function (e) {
              return new Number(e.o);
            },
            ["asc"]
          );
          
          lh.each(v, (e,i) => {
            console.log('i:',i,'o:',e.o);
            let ct = e.l.substring(0,e.l.indexOf("("));
            if (e.o=="1") { //il primo termine della regola individua la tabella principale
              nv = 0;
              let data = e.c.split("|");
              let vars = e.v.split("|");
    
              mainTable={table:data[0],pk:data[1].substring(data[1].lastIndexOf('.')+1,100),var:vars[0]}; //salvo la PK con il solo nome del campo in prospettiva della subquery eventuale
              console.log("mainTable",mainTable);
              
              vq[nq].cte = knex(mainTable.table);
              vq[nq].main = knex.with("cte",vq[nq].cte).from("cte");
              
              let fn = assignFieldMapping([data[1]], vars, fieldMapping);
              vq[nq].cte.select(data[1]+" as "+fn.alias);   //metto il campo in proiezione nella query join 
              vq[nq].main.select("cte."+fn.alias); //metto il campo in proiezione nella query principale
              fields2gb.push(fn.alias);
            
              tabMapping.set(e.v, data[0]); //mappa la variabile principale per le join (ordine 1)
              tree.addVertex(vars[0],data); nv++; //aggiunge il nodo della tabella all'albero delle join
            } else {
              console.log("ct:",ct );
              let vars;
              let data;
              let fn;
              let el;
              if (e.v && e.c) {
                vars = e.v.split("|");
                data = e.c.split("|");

                switch (ct) {
                  case "ObjectPropertyInstance": 
                    //vars = e.v.split("|");
                    //data = e.c.split("|");
                    if (data.length==4) {
                      assignMapping(data,vars,tabMapping);   //mappa le variabili
                      tree.addVertex(vars[0]); nv++; //aggiunge il nodo della tabella all'albero delle join
                      tree.addVertex(vars[1],data); nv++; //aggiunge il nodo della tabella all'albero delle join
                      tree.addEdge(vars[0],vars[1],true); //arco orientato
                      //console.log(data);
                    } else {
                      let data1 = data.slice(0,4); //spezza in due join i dati
                      assignMapping(data1,[vars[0], vars[0]+"-"+vars[1]],tabMapping);   //mappa le variabili
                      tree.addVertex(vars[0]); nv++; //aggiunge il primo nodo della tabella all'albero delle join
                      tree.addVertex(vars[0]+"-"+vars[1],data1); nv++; //aggiunge il secondo nodo della tabella all'albero delle join (cross)
                      tree.addEdge(vars[0],vars[0]+"-"+vars[1],true); //arco orientato
                      let data2 = data.slice(4); //spezza in due join i dati
                      assignMapping(data2,[vars[0]+"-"+vars[1],vars[1]],tabMapping);   //mappa le variabili
                      tree.addVertex(vars[1],data2); nv++; //aggiunge il terzo nodo della tabella all'albero delle join
                      tree.addEdge(vars[0]+"-"+vars[1],vars[1],true); //arco orientato
                    }

                    break;
                  case "DataPropertyEqualsTo":
                  case "DataPropertyNotEqualsTo":
                  case "DataPropertyGreaterThan":
                  case "DataPropertyLessThan": 
                    //vars = e.v.split("|");
                    //data = e.c.split("|"); 

                    fn = assignFieldMapping(data, vars, fieldMapping);
                    if (fn.prj) {
                      vq[nq].cte.select(fn.field+" as "+fn.alias);   //metto il campo in proiezione nella query join 
                      vq[nq].main.select("cte."+fn.alias); //metto il campo in proiezione nella query principale
                      fields2gb.push(fn.alias); //lo segno tra quelli che possono andare in GROUP BY
                    }

                    //conservo i dati per la scrittura di WHERE/HAVING
                    if (ct=="DataPropertyEqualsTo")
                      hwc.push([e.b, fn.alias, vars[1], '=', '<>']) 
                    else
                    if (ct=="DataPropertyNotEqualsTo")
                      hwc.push([e.b, fn.alias, vars[1], '<>', '=']) 
                    else
                    if (ct=="DataPropertyGreaterThan")
                      hwc.push([e.b, fn.alias, vars[1], '>', '<=']) 
                    else
                    if (ct=="DataPropertyLessThan")
                      hwc.push([e.b, fn.alias, vars[1], '<', '>=']) 
                    else {}

                    break;
                  case "DataPropertySum":  
                    gb = true;
                    //vars = e.v.split("|");
                    //data = e.c.split("|"); 

                    fn = assignFieldMapping(data, vars, fieldMapping);
                    if (fn.prj) {
                      vq[nq].cte.select(fn.field+" as "+fn.alias); //metto il campo in proiezione
                      vq[nq].main.select("SUM(cte." + fn.alias + ") as " + fn.alias+"_SUM"); //metto il campo in proiezione come SUM
                    }
                    varMapping.set(vars[1],fn.alias+"_SUM"); //salvo l'alias legato alla variabile di appoggio

                    break;
                  case "DataPropertyValue":  
                    //vars = e.v.split("|");
                    //data = e.c.split("|");
                    fn = assignFieldMapping(data, vars, fieldMapping);
                    if (fn.prj) {
                      vq[nq].cte.select(fn.field+" as "+fn.alias); //metto il campo in proiezione
                      vq[nq].main.select("cte."+fn.alias); //metto il campo in proiezione
                      fields2gb.push(fn.alias); //lo segno tra quelli che possono andare in GROUP BY
                    }
                    varMapping.set(vars[1],fn.alias); //salvo l'alias legato alla variabile di appoggio

                    break;  
                  case "DataPropertyListSomeIn":  
                    //vars = e.v.split("|");
                    //data = e.c.split("|");
                    gb=true;
                    fn = assignFieldMapping(data, vars, fieldMapping);
                    if (fn.prj) {
                      vq[nq].cte.select(fn.field+" as "+fn.alias); //metto il campo in proiezione
                      vq[nq].main.select("cte."+fn.alias); //metto il campo in proiezione
                      fields2gb.push(fn.alias); //lo segno tra quelli che possono andare in GROUP BY
                    }

                    el = vars.slice(1).map((e) => "'"+e+"'").join(",");
                    scw.push([e.b, fn.alias, el, "="]); //conservo i dati per la scrittura delle condizioni HAVING sulla quey annidata

                    break;   
                  case "DataPropertyListAllNotIn":
                    //vars = e.v.split("|");
                    //data = e.c.split("|");
                    gb=true;
                    fn = assignFieldMapping(data, vars, fieldMapping);
                    if (fn.prj) {
                      vq[nq].cte.select(fn.field+" as "+fn.alias); //metto il campo in proiezione
                      vq[nq].main.select("cte."+fn.alias); //metto il campo in proiezione
                      fields2gb.push(fn.alias); //lo segno tra quelli che possono andare in GROUP BY
                    }

                    el = vars.slice(1).map((e) => "'"+e+"'").join(",");
                    scw.push([e.b, fn.alias, el, ">"]); //conservo i dati per la scrittura delle condizioni HAVING sulla quey annidata

                    break;
                  case "ValuesSubSet":  
                    //vars = e.v.split("|");
                    //data = e.c.split("|");

                    //il ragionatore in c ovvero "data" passa la PK della tabella, il confronto infatti avviene con riferimento ad una specifica classe
                    //perché riguarda insiemi di istanze di quella classe che devono essere uno contenuto nell'altro. 
                    //Ad esempio : ValuesSubSet(ContrattiPubblici_CategoriaMerceologica,v4,v5) ha senso perchè stiamo confrontand due insiemi
                    //di istanze della classe ContrattiPubblici_CategoriaMerceologica (tramite la loro PK) individuati dalle variabili v4 e v5

                    //devo proiettare la PK con i corretti ALIAS per poi fare la verifica
                    //console.log("ValuesSubSet-----");
                    let ot=tabName(data[0]);
                    let nt=aliasTabName(vars[0],[null,ot],tabMapping); //riuso la funzione che costruisce l'eventuale alias in fase di join
                    let fn1 = assignFieldMapping([nt?data[0].replace(ot,nt):data[0]], [vars[0]], fieldMapping);
                    //console.log("fn1:",fn1);
                    if (fn1.prj) {
                      vq[nq].cte.select(fn1.field+" as "+fn1.alias); //metto il campo in proiezione
                      vq[nq].main.select("cte."+fn1.alias); //metto il campo in proiezione
                      fields2gb.push(fn1.alias); //lo segno tra quelli che possono andare in GROUP BY
                    }
                    nt=aliasTabName(vars[1],[null,ot],tabMapping); //riuso la funzione che costruisce l'eventuale alias in fase di join
                    let fn2 = assignFieldMapping([nt?data[0].replace(ot,nt):data[0]], [vars[1]], fieldMapping);
                    //console.log("fn2:",fn2);
                    if (fn2.prj) {
                      vq[nq].cte.select(fn2.field+" as "+fn2.alias); //metto il campo in proiezione
                      vq[nq].main.select("cte."+fn2.alias); //metto il campo in proiezione
                      fields2gb.push(fn2.alias); //lo segno tra quelli che possono andare in GROUP BY
                    }
                    //console.log("-----------------");
                    gb = true;

                    //non c'è bisogno di conservare i dati e piazzare la condizione alla fine si può fare subito (?)
                    let pkj = fieldMapping.get(mainTable.table+"."+mainTable.pk); //assumo che la chiave di riferimento è quella della tabella principale proiettata nella cte 
                    //console.log("PKJ", pkj, mainTable);
                    vq[nq].main.having(knex.raw("COUNT(DISTINCT "+fn1.alias+") > COUNT(DISTINCT CASE WHEN "+fn1.alias+" IN ( SELECT "+fn2.alias+" FROM cte t2 WHERE t2."+pkj+" = cte."+pkj+" ) THEN "+fn1.alias+" END )"));

                    break;
                  case "ClassInstance":  

                    //per ora non fa nulla ma si potrebbe fare in modo che questo costrutto passi in c (attraverso il ragionatore) i riferimenti
                    //alla PK della tabella che potrebbe essere usata per seplificare qualche passaggio dei casi precedenti

                    break;

                  default:
                    console.log("**COSTRUTTO NON RICONOSCIUTO**", ct);
                    break;
                  
                }

              } else {
                console.log("**dati di mapping non presenti**", ct);
              }
              
            }
          });

          console.log("## tree:",tree);
          //faccio una visita in ampiezza dell'albero generato delle ObjectPropertyInstance
          let bfs = tree.bfs(mainTable.var); 
          console.log("## BFS:",bfs);
          
          //per ogni nodo (tranne la radice) inserisco una join
          bfs.forEach((e,i) => {
            let data = tree.getData(e);
            //let mapp = tabMapping.get(e).split('|'); //se esiste un valore vicino alla tabella nel mapping deve essere usato per l'alias 
            console.log("  bfs["+i+"]:"+ data);

            if (data[1] && data[2] && data[3]) {
              let ot = data[1]; //nome originale dela tabella da joinare 
              let nt = aliasTabName(e,data,tabMapping) //eventuale alias

              //faccio la join mettendo l'alias eventuale
              if (nt) vq[nq].cte.join({ [nt] : data[1] },data[2].replace(ot,nt), "=" , data[3].replace(ot,nt)); //nella join uso l'eventuale alias
              else vq[nq].cte.join(data[1], data[2] , '=' , data[3]);
            }
          });

          //qui devo capire quando fare una WHERE e quando una HAVING a seconda che ci sia stato o meno una group by
          hwc.forEach((e,i) => {
            console.log("###("+i+") ",e);
            if (!e[2].startsWith("BR_var")) e[2]="'"+e[2]+"'"; //se è una variabile o un valore metto gli apici
            else e[2] = varMapping.get(e[2]); //se è una variabile recupero l'alias del campo corrispondente
            if (gb)
              if (fields2gb.includes(e[1]))
                if (e[0]=="true") vq[nq].main.having(knex.raw(e[1]+e[3]+e[2]));
                else vq[nq].main.having(knex.raw(e[1]+e[4]+e[2]));
              else {
                console.log("*errore nella costruzione della HAVING*",ct);
              }  
            else   
              if (e[0]=="true") vq[nq].main.where(knex.raw(e[1]+e[3]+e[2]));
              else vq[nq].main.where(knex.raw(e[1]+e[4]+e[2]));
          });

          //group by su tutti i campi proiettati tranne quelli aggregato
          if (gb) vq[nq].main.groupBy(fields2gb.join(','));

          //aggiungo le HAVING per sum case when
          if (scw.length>0) {
            let scws = "";
            scw.forEach((e,i) => {
              if (scws.length==0) scws = "SUM(CASE WHEN "+e[1]+" IN ("+e[2]+") THEN 1 END) "+e[3]+" 0"
              else scws += " OR SUM(CASE WHEN "+e[1]+" IN ("+e[2]+") THEN 1 END) "+e[3]+" 0"; 

            });
            vq[nq].main.having(knex.raw("("+scws+")"));
          }

          console.log("## tabMapping    :",tabMapping);
          console.log("## fieldMapping  :",fieldMapping);
          console.log("## varMapping    :",varMapping);
          console.log("## gb            :",gb,fields2gb);
          console.log("## hwc           :",hwc)
          console.log("## scw           :",scw)

          //stampo la query
          console.log("\nQ["+nq+"] ", vq[nq].main.toString() + "\n" );

        })

        return inferred;
    
      }

}

const meta2sqlInstance = new Meta2SQL();
module.exports = {
    applyRulesCustom: meta2sqlInstance.applyRulesCustom.bind(meta2sqlInstance)
};