const N3 = require('n3');
const Comunica = require('@comunica/query-sparql');

class Store {

  //carica nello store delle triple
  async storeLoad(store, data) {
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
  async executeStoreQuery  (query, store) {
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
  getResultSet(vbindings) {
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

}

const storeInstance = new Store();
module.exports = {
  getResultSet: storeInstance.getResultSet.bind(storeInstance),
  executeStoreQuery: storeInstance.executeStoreQuery.bind(storeInstance),
  storeLoad: storeInstance.storeLoad.bind(storeInstance)
};