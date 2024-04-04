const express = require('express')

const { Worker } = require('worker_threads')

const app = express()
const port = 3000

app.get('/', (req, res) => {

    const worker = new Worker("./ex-worker.js", {workerData: {n:req.query.n}} );

    worker.once("message", result => {
        res.send({res:result})
    });

    worker.on("error", error => {
        console.log(error);
    });

    worker.on("exit", exitCode => {
        console.log(`${worker.threadId} exited with code ${exitCode}`);
    })
})

app.get('/hello', (req, res) => {
    res.send('Hello')
})

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})