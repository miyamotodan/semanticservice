
const N3 = require('n3');
const eye = require('eyereasoner');
const wt = require("worker_threads");
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

  if (wt.workerData.mode==2)
    applyRules2(wt.workerData.data).then(inferred => wt.parentPort.postMessage(inferred));
  else
    applyRules(wt.workerData.data).then(inferred => wt.parentPort.postMessage(inferred));

    