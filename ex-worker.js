const wt = require("worker_threads");

console.log("wt", wt.threadId, wt.workerData);

runWhileLoop(wt.workerData.n).then( (rwl) => {

  wt.parentPort.postMessage(rwl);

});


async function runWhileLoop(n) {

    await new Promise(resolve => setTimeout(resolve, 5000));

    let fac = 1;
    for (let i = 1; i <= n; i++) {
      fac = fac*i;
    }
    return fac;
}