import sqlite3 from 'sqlite3';
import { open, Database } from 'sqlite';
import fetch from 'node-fetch';


// 🔹 Uniswap V2 & V3 工厂合约地址
const UNISWAP_V2_FACTORY = "0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6";
const UNISWAP_V3_FACTORY = "0xdB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7";

// 🔹 PairCreated & PoolCreated 事件的 keccak 哈希
const PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9";
const POOL_CREATED_TOPIC = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";

// 🔹 轮询多个 RPC 以加速查询
let RPC_URLS = [
        "https://binance.llamarpc.com",
        "https://bsc-dataseed.bnbchain.org",
        "https://bsc-dataseed1.defibit.io",
        "https://bsc-dataseed1.ninicoin.io",
        "https://bsc-dataseed2.defibit.io",
        "https://bsc-dataseed3.defibit.io",
        "https://bsc-dataseed4.defibit.io",
        "https://bsc-dataseed2.ninicoin.io",
        "https://bsc-dataseed3.ninicoin.io",
        "https://bsc-dataseed4.ninicoin.io",
        "https://bsc-dataseed1.bnbchain.org",
        "https://bsc-dataseed2.bnbchain.org",
        "https://bsc-dataseed3.bnbchain.org",
        "https://bsc-dataseed4.bnbchain.org",
        "https://0.48.club",
        "https://bsc-pokt.nodies.app",
        "https://bsc-mainnet.nodereal.io/v1/64a9df0874fb4a93b9d0a3849de012d3",
        "https://binance.nodereal.io",
        "https://bsc.rpc.blxrbdn.com",
        "https://bsc-rpc.publicnode.com",
        "https://bsc-mainnet.public.blastapi.io",
        "https://api.zan.top/bsc-mainnet",
        "https://bsc.blockrazor.xyz"
    
];

interface Log {
    topics: string[];
    data: string;
    address: string;
}

class UniswapFetcher {
    private db: Database | null = null;
    private currentRpcIndex = 0;

    constructor() {
        this.initDB();
    }

    private async initDB() {
        this.db = await open({
            filename: 'uniswap_logs.db',
            driver: sqlite3.Database
        });

        await this.db.exec(`
            CREATE TABLE IF NOT EXISTS uniswap_pools (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token0 BLOB,
                token1 BLOB,
                type TEXT,
                address BLOB
            )
        `);

        // SQLite 性能优化
        await this.db.exec(`
            PRAGMA synchronous = NORMAL;
            PRAGMA journal_mode = WAL;
            PRAGMA cache_size = -50000;
        `);
    }

    private getRpc(): string {
        const rpc = RPC_URLS[this.currentRpcIndex];
        this.currentRpcIndex = (this.currentRpcIndex + 1) % RPC_URLS.length;
        return rpc;
    }

    private removeRpc(rpc: string) {
        RPC_URLS = RPC_URLS.filter(url => url !== rpc);
        console.log(`Removed failing RPC: ${rpc}`);
        console.log(`Remaining RPCs: ${RPC_URLS.length}`);
    }

    private async tryFetchLogs(fromBlock: number, toBlock: number, topic: string, rpc: string): Promise<[Log[] | null, string | null]> {
        const payload = {
            jsonrpc: "2.0",
            method: "eth_getLogs",
            params: [{
                fromBlock: `0x${fromBlock.toString(16)}`,
                toBlock: `0x${toBlock.toString(16)}`,
                topics: [topic]
            }],
            id: 1
        };

        try {
            const response = await fetch(rpc, {
                method: 'POST',
                body: JSON.stringify(payload),
                headers: { 'Content-Type': 'application/json' }
            });

            if (response.status === 429) {
                return [null, "rate_limited"];
            }
            const result = await response.json() as any;
            if ("error" in result && typeof result.error === 'object' && result.error !== null) {
                if (typeof result.error.message === 'string' && result.error.message.toLowerCase().includes('rate')) {
                    return [null, "rate_limited"];
                }
                return [null, `rpc_error: ${JSON.stringify(result.error)}`];
            }

            return [result.result, null];

        } catch (e) {
            return [null, String(e)];
        }
    }

    private async saveLogs(logs: Log[]) {
        if (!this.db) throw new Error("Database not initialized");

        for (const log of logs) {
            const eventTopic = log.topics[0];
            console.log(log.topics[0]);
            const poolType = eventTopic === PAIR_CREATED_TOPIC ? "V2" : 
                           eventTopic === POOL_CREATED_TOPIC ? "V3" : null;

            if (!poolType) {
                console.log(`Unknown event topic: ${eventTopic}`);
                continue;
            }
            
            try {
                const token0 = Buffer.from(log.topics[1].slice(-40), 'hex');
                const token1 = Buffer.from(log.topics[2].slice(-40), 'hex');
                const address = Buffer.from(log.data.slice(26, 66), 'hex');

                console.log(`Token0: 0x${token0.toString('hex')}`);
                console.log(`Token1: 0x${token1.toString('hex')}`);
                console.log(`Pool Address: 0x${address.toString('hex')}`);
                console.log(`Type: ${poolType}`);
                console.log("-".repeat(50));

                await this.db.run(
                    `INSERT INTO uniswap_pools (token0, token1, type, address) VALUES (?, ?, ?, ?)`,
                    [token0, token1, poolType, address]
                );
            } catch (e) {
                console.error("Error processing log:", log);
                console.error("Error details:", e);
            }
        }
    }

    private async getLatestBlock(): Promise<number> {
        const rpc = this.getRpc();
        const payload = {
            jsonrpc: "2.0",
            method: "eth_blockNumber",
            params: [],
            id: 1
        };

        try {
            const response = await fetch(rpc, {
                method: 'POST',
                body: JSON.stringify(payload),
                headers: { 'Content-Type': 'application/json' }
            });
            const result = await response.json() as { result?: string };
            if (result.result) {
                return parseInt(result.result, 16);
            }
        } catch (e) {
            console.error(`Error getting latest block: ${e}`);
        }
        return 0;
    }

    public async fetchLogs(fromBlock: number, toBlock: number, topic: string): Promise<Log[]> {
        let retryCount = 0;
        const maxRetries = 1;

        while (retryCount <= maxRetries) {
            const rpc = this.getRpc();
            const [logs, error] = await this.tryFetchLogs(fromBlock, toBlock, topic, rpc);

            if (logs) return logs;

            if (error === "rate_limited") {
                const waitTime = 0.2 * (retryCount + 1);
                await new Promise(resolve => setTimeout(resolve, waitTime * 1000));
                retryCount++;
            } else {
                this.removeRpc(rpc);
                if (RPC_URLS.length === 0) {
                    throw new Error("No more RPC nodes available!");
                }
            }
        }

        return [];
    }

    public async batchFetchAndStore(startBlock: number, endBlock: number, step = 40000) {
        const tasks: Promise<Log[]>[] = [];
        const totalBlocks = endBlock - startBlock;
        let blocksProcessed = 0;

        for (let block = startBlock; block < endBlock; block += step) {
            const currentEnd = Math.min(block + step - 1, endBlock);
            tasks.push(this.fetchLogs(block, currentEnd, PAIR_CREATED_TOPIC));
            tasks.push(this.fetchLogs(block, currentEnd, POOL_CREATED_TOPIC));

            blocksProcessed += step;
            const progress = (blocksProcessed / totalBlocks) * 100;
            console.log(`Progress: ${progress.toFixed(2)}% (Block ${block.toLocaleString()} to ${currentEnd.toLocaleString()})`);

            if (tasks.length >= 30) {
                const results = await Promise.all(tasks);
                for (const logs of results) {
                    if (logs.length > 0) {
                        await this.saveLogs(logs);
                    }
                }
                tasks.length = 0;
            }
        }

        if (tasks.length > 0) {
            const results = await Promise.all(tasks);
            for (const logs of results) {
                if (logs.length > 0) {
                    await this.saveLogs(logs);
                }
            }
        }
    }

    public async start() {
        const latestBlock = await this.getLatestBlock();
        console.log(`Latest BSC block: ${latestBlock.toLocaleString()}`);
        await this.batchFetchAndStore(0, latestBlock);
        await this.db?.close();
    }
}

// 运行程序
const fetcher = new UniswapFetcher();
fetcher.start().catch(console.error);