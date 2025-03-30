import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import Graph from 'graphology';

// 数据库路径
const DB_PATH = './uniswap_logs.db';

// 构建 Token 交换图
async function buildGraph(): Promise<[Graph, Graph]> {
  const db = await open({
    filename: DB_PATH,
    driver: sqlite3.Database
  });

  const v2_graph = new Graph();
  const v3_graph = new Graph();
  const pairs = await db.all('SELECT token0, token1, type, address FROM uniswap_pools');

  for (const { token0, token1, type, address } of pairs) {
    const graph = type === 'V2' ? v2_graph : v3_graph;
    const _token0 = '0x' + Buffer.from(token0).toString('hex');
    const _token1 = '0x' + Buffer.from(token1).toString('hex');
    const _address = '0x' + Buffer.from(address).toString('hex');
    graph.mergeNode(_token0);
    graph.mergeNode(_token1);
    if (!graph.hasEdge(_token0, _token1)) {
        graph.addEdge(_token0, _token1, { pool: _address});
      }
  }
  await db.close();
  return [v2_graph, v3_graph ];
}

// 深度优先搜索 (DFS) 查找所有路径
function findAllPaths(graph: Graph, start: string, end: string): string[][] {
    const results: string[][] = [];
    const visited = new Set<string>();
    const MAX_PATH_LENGTH = 3;  // 设置最大路径长度为3
  
    function dfs(path: string[], node: string) {
      // 如果当前路径长度已经达到最大值，且还未到达目标节点，则返回
      if (path.length >= MAX_PATH_LENGTH && node !== end) {
        return;
      }
  
      if (node === end) {
        results.push([...path, node]);
        return;
      }
  
      visited.add(node);
      for (const neighbor of graph.neighbors(node)) {
        if (!visited.has(neighbor)) {
          dfs([...path, node], neighbor);
        }
      }
      visited.delete(node);
    }
  
    dfs([], start);
    return results;
  }

// 运行查找
async function findSwapPaths(tokenA: string, tokenB: string, type: string) {
  const [v2_graph, v3_graph ] = await buildGraph();
  const graph = type === 'V2' ? v2_graph : v3_graph;

  if (!graph.hasNode(tokenA) || !v2_graph.hasNode(tokenB)) {
    console.log('Tokens not found in the graph.');
    return null;
  }

  const paths = findAllPaths(graph, tokenA, tokenB);
  console.log(`Found ${paths.length} paths from ${tokenA} to ${tokenB}:`);
  console.log(paths);
  return paths;
}

// 示例：寻找 WETH 和 DAI 之间的所有 swap 路径
const temp1 = '0x55d398326f99059ff775485246999027b3197955';
const temp2 = '0xe9e7cea3dedca5984780bafc599bd69add087d56';

findSwapPaths(temp1, temp2, 'V2');
