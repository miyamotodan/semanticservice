//classe che implementa alcune funzioni basiche per fare esplrazione di una struttura ad albero/grafo
class Graph {
    constructor() {
        this.adjacencyList = {};
        this.dataSet = new Map();
    }

    addVertex(vertex, data) {

        //console.log(">>> addVertex:",vertex,data);
        //se il nodo non esiste lo aggiunge alla lista delle diacenze
        if (!this.adjacencyList[vertex]) {
            this.adjacencyList[vertex] = [];
            
        }
        //se i dati non ci sono li mette
        if (!this.dataSet.get(vertex)) this.dataSet.set(vertex,data);
    }

    addEdge(vertex1, vertex2, oriented) {

        //console.log(">>> addEge:",vertex1,vertex2,oriented);
        if (!this.adjacencyList[vertex1].includes(vertex2)) this.adjacencyList[vertex1].push(vertex2);
        if (!oriented) // Se il grafo è non orientato
            if (!this.adjacencyList[vertex2].includes(vertex1)) this.adjacencyList[vertex2].push(vertex1); 
    }

    getData(vertex) {
        return this.dataSet.get(vertex);
    }

    // DFS traversal using recursion
    dfs(start) {
        const result = [];      // List to store the result of DFS traversal
        const visited = {};     // Object to store visited vertices
        const adjacencyList = this.adjacencyList;

        // Helper function to perform DFS recursively
        const dfsRecursion = (vertex) => {
            if (!vertex) return;  // Base case if vertex is not valid
            visited[vertex] = true;    // Mark the vertex as visited
            result.push(vertex);       // Push vertex to the result list
            adjacencyList[vertex].forEach(neighbor => { // Visit adjacent vertices
                if (!visited[neighbor]) {
                    dfsRecursion(neighbor);  // Recurse on the adjacent vertex
                }
            });
        }

        dfsRecursion(start);  // Kickstart the DFS

        return result;
    }

    // BFS traversal using iteration
    bfs(start) {
        const result = [];       // List to store the result of BFS traversal
        const visited = {};      // Object to store visited vertices
        const queue = [start];   // Queue to manage the BFS process
        visited[start] = true;   // Mark the starting vertex as visited

        while (queue.length) {
            const vertex = queue.shift(); // Dequeue a vertex
            result.push(vertex);          // Push it to the result list

            this.adjacencyList[vertex].forEach(neighbor => {
                if (!visited[neighbor]) {
                    visited[neighbor] = true; // Mark the neighbor as visited
                    queue.push(neighbor);     // Enqueue the neighbor
                }
            });
        }

        return result;
    }
}

// Esportazione del modulo
module.exports = Graph;
