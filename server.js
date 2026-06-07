(async () => {
    const { fork } = await import("child_process");
    const { WebSocketServer } = await import("ws");
    const { pack, unpack } = await import("msgpackr");
    const http = await import("http");


    const PROXIES = [
        "http://I8E74HOnCybnFnQy:bURF2IdrwWusS9Yo@94.54.183.187:61235",
        "http://be05c50b3c0a8d57:0892991b82bc0766@98.159.44.214:61234",
        "http://ASSFmROdeYbTLN0t:ujc8uMQ7PxodFZS3@149.18.100.31:61234",
        "http://195d189496437e4d:47e06c2cb6afef5f@149.18.100.96:61234",
        "http://84062085562b67c5:735bf6950a696e60@149.18.100.144:61234",
        "http://AznUET2bRAb6OMNB:Wh7vaJbqoBGImKQi@149.18.100.163:61234",
        "http://ac9dec5afac52024:298479fb300d0f5c@149.18.100.186:61234",
        "http://c4nqK7LFLgoKPzRH:BSMz6jQVRcjUvs8M@149.18.117.23:61234",
        "http://kDsB68EfZqwHebUv:aqJsF7YXesrmMIHL@194.54.183.98:61235",
        "http://t2kFDnkoooCWshME:8K8xN51Uz0t6qJUA@194.54.183.220:61235",
        "http://c462bb62f4990203:1bc8718bb918c9ff@149.18.117.66:61234",
        "http://efzCHaLmQeMthC3I:zv1M41NmqVlGAdB9@194.54.183.13:61235",
        "http://xktJ19YfhtRhHmKl:WxMy4UsxxIj3KswI@149.18.117.235:61234",
        "http://NLTvQORyZPwyYp9p:akVyXJ9Xa3C7Gs0S@194.54.183.33:61235",
        "http://cZA7swJQy1LHN1Bo:Y1J3fsHKPgRkC5MT@149.18.117.134:61234",
        "http://FPMx5qijZGAWNr3p:WNaDsj2DP6ldpCaG@194.54.183.26:61235",
        "http://t1c7a2VBhqZKjHUN:Nbiy1Z72aAJwqE9R@194.54.183.30:61235",
        "http://39570e2e88934dab:13b79b7dd3de0163@149.18.117.119:61234",
        "http://I8E74HOnCybnFnQy:bURF2IdrwWusS9Yo@194.54.183.187:61235",
        // Proxies imported from working.json
        "http://1.231.81.166:3128",
        "http://101.255.106.253:3128",
        "http://101.255.119.25:8080",
        "http://101.32.100.83:8080",
        "http://102.207.128.2:8080",
        "http://103.105.54.185:8080",
        "http://103.113.152.73:14158",
        "http://103.118.102.98:80",
        "http://103.136.107.60:100",
        "http://103.145.34.112:1111",
        "http://103.147.118.67:8080",
        "http://103.147.247.193:8080",
        "http://103.152.140.182:8090",
        "http://103.154.230.246:8090",
        "http://103.155.168.164:8299",
        "http://103.156.14.54:8080",
        "http://103.157.79.155:1818",
        "http://103.158.127.199:57413",
        "http://103.158.242.62:83",
        "http://103.159.195.193:3125",
        "http://103.159.96.195:8080",
        "http://103.162.221.162:3125",
        "http://103.165.155.195:8080",
        "http://103.174.131.249:3128",
        "http://103.176.201.10:8080",
        "http://103.189.119.239:8080",
        "http://103.191.196.212:8080",
        "http://103.192.174.154:8080",
        "http://103.194.175.51:7777",
        "http://103.217.224.75:3125",
        "http://103.220.23.127:8080",
        "http://103.234.35.141:8080",
        "http://103.235.153.2:3889",
        "http://103.247.23.37:1111",
        "http://103.39.51.130:1080",
        "http://103.55.22.236:8080",
        "http://103.56.205.84:8080",
        "http://103.90.66.19:8087",
        "http://103.97.140.17:8080",
        "http://111.92.88.27:3128",
        "http://112.78.134.134:7777",
        "http://113.192.1.114:8181",
        "http://113.192.31.90:8080",
        "http://114.130.92.61:8090",
        "http://115.127.105.163:6699",
        "http://116.104.252.1:2105",
        "http://116.80.64.44:3172",
        "http://119.148.47.235:1048",
        "http://121.101.129.131:8080",
        "http://129.222.204.27:10000",
        "http://13.201.56.14:3128",
        "http://130.49.218.108:1080",
        "http://131.222.210.39:8080",
        "http://131.222.252.102:8080",
        "http://134.249.86.47:8080",
        "http://135.181.113.216:16379",
        "http://138.124.97.240:3128",
        "http://138.2.156.101:3128",
        "http://139.5.75.83:8080",
        "http://14.225.240.23:8562",
        "http://144.124.227.88:3128",
        "http://144.24.41.48:9999",
        "http://148.230.4.241:999",
        "http://148.251.86.68:16379",
        "http://154.223.188.202:1194",
        "http://160.19.19.122:8090",
        "http://160.238.65.2:3128",
        "http://160.238.65.6:3128",
        "http://160.238.65.9:3128",
        "http://165.16.27.43:1981",
        "http://165.227.133.230:8888",
        "http://168.110.52.228:3128",
        "http://171.247.158.101:8080",
        "http://173.212.245.136:8888",
        "http://174.138.3.101:8443",
        "http://176.111.37.216:39811",
        "http://176.88.65.187:8080",
        "http://179.1.113.129:999",
        "http://180.191.36.250:8081",
        "http://181.209.96.154:999",
        "http://181.78.44.63:999",
        "http://182.160.114.106:12331",
        "http://182.253.69.95:8080",
        "http://185.41.152.110:3128",
        "http://186.148.31.20:999",
        "http://187.72.215.33:3128",
        "http://190.119.90.114:8080",
        "http://190.242.60.137:999",
        "http://190.52.110.63:999",
        "http://190.52.110.95:999",
        "http://190.60.39.226:999",
        "http://190.60.44.109:999",
        "http://192.99.8.15:8850",
        "http://193.43.149.85:8080",
        "http://195.158.8.123:3128",
        "http://195.25.20.155:3128",
        "http://196.204.80.111:1981",
        "http://199.127.62.89:3129",
        "http://2.78.60.10:3129",
        "http://20.210.39.153:8561",
        "http://20.210.76.175:8561",
        "http://20.210.76.178:8561",
        "http://20.27.11.248:8561",
        "http://20.27.13.35:8561",
        "http://20.27.14.220:8561",
        "http://20.27.15.49:8561",
        "http://20.78.26.206:8561",
        "http://200.174.198.32:8888",
        "http://200.34.227.28:8080",
        "http://200.70.35.2:8080",
        "http://201.230.15.57:999",
        "http://201.62.125.142:8080",
        "http://202.5.33.33:1101",
        "http://202.56.163.109:8080",
        "http://203.30.9.8:8443",
        "http://216.236.30.14:7443",
        "http://216.9.225.157:3128",
        "http://217.182.195.221:30004",
        "http://217.199.144.80:8080",
        "http://217.76.46.230:8080",
        "http://27.147.177.74:55",
        "http://3.90.0.161:8000",
        "http://36.88.249.106:80",
        "http://36.95.223.193:8080",
        "http://37.49.224.15:3128",
        "http://38.127.179.42:37234",
        "http://38.156.71.1:999",
        "http://38.159.37.173:999",
        "http://38.190.100.106:999",
        "http://38.194.246.34:999",
        "http://38.226.49.203:8080",
        "http://38.49.142.174:999",
        "http://38.7.195.50:999",
        "http://38.75.82.217:999",
        "http://4.221.164.109:443",
        "http://41.78.168.118:8080",
        "http://43.128.145.26:1080",
        "http://43.156.175.175:8080",
        "http://43.246.201.53:23654",
        "http://43.252.236.158:8080",
        "http://45.158.10.98:8085",
        "http://45.159.79.101:3128",
        "http://45.168.236.54:3128",
        "http://45.224.153.245:999",
        "http://45.230.170.30:999",
        "http://45.84.222.25:1080",
        "http://46.203.233.116:3128",
        "http://47.238.203.170:50002",
        "http://62.171.168.211:3128",
        "http://69.75.140.157:8080",
        "http://81.0.49.104:20500",
        "http://83.222.7.205:3128",
        "http://85.234.100.149:8080",
        "http://85.238.106.201:28089",
        "http://89.46.42.11:8080",
        "http://95.3.69.222:8080"
    ];
    const prod = false;

    // HTTP SERVER
    const server = http.createServer((req, res) => {
        res.writeHead(426, { "Content-Type": "text/plain" });
        res.end("lll elk ez big fat noob");
    });


    // WS SERVER
    function randint(a, b) {
        return Math.floor(Math.random() * (b - a + 1)) + a;
    }

    const sessions = new Map();
    const wss = new WebSocketServer({ server });

    // ── Centralized worker fork helper ─────────────────────────────────────
    // Creates a child process, wires up events, stores the spawn config for
    // re-forking on force respawn, and sends initial tank + start messages.
    function forkWorker(session, ws, packet, config, tankOverride) {
        const worker = fork("index.js", []);

        // Store config on worker so "K" (force respawn) can re-fork with same settings
        worker._spawnConfig = config;
        worker._spawnTank = tankOverride || session.tank;

        worker.on('exit', () => {
            session.workers = session.workers.filter(w => w !== worker);
            if (session.scanWorkers) session.scanWorkers = session.scanWorkers.filter(w => w !== worker);
        });
        worker.on('error', (err) => {
            // EPIPE / ECONNRESET are expected on Codespace teardowns
            if (err.code === 'EPIPE' || err.code === 'ECONNRESET') {
                console.warn(`Worker error (${err.code}), cleaning up.`);
            } else {
                console.error('Worker error:', err);
            }
        });
        worker.on('disconnect', () => {
            // Expected when worker exits or Codespace recycles — not fatal
        });

        worker.on("message", (msg) => {
            if (!ws || ws.readyState !== ws.OPEN) return;
            try {
                if (msg && msg.type === "report") {
                    packet("R", msg);
                } else if (msg && msg.type === "scanFound") {
                    packet("R", msg.report);
                    packet("X");
                }
            } catch (err) {
                console.warn('Forwarding worker message failed:', err.code || err.message);
            }
        });

        session.workers.push(worker);

        // Send tank selection
        if (worker.connected) {
            const tank = tankOverride || session.tank;
            try {
                worker.send({ type: "tankselect", tank: tank });
            } catch (_) {}
        }

        // Send start command
        try {
            if (worker.connected) {
                worker.send({ type: "start", config: config });
            }
        } catch (err) {
            console.warn('Worker start send failed:', err.code || err.message);
        }

        return worker;
    }

    wss.on("connection", (ws, req) => {
        const addr = req.socket.remoteAddress;
        console.log(addr, "connected");

        // Prevent unhandled WS errors from crashing the server (Codespace resilience)
        ws.on('error', (err) => {
            console.warn(`WebSocket error from ${addr}:`, err.code || err.message);
        });

        // Initialize or retrieve session for this IP
        if (!sessions.has(addr)) {
            sessions.set(addr, {
                workers: [],
                tank: "auto6",
                tanks: [],
                tankIdx: 0,
                proxyIdx: 0,
                swarmMode: "normal"
            });
        }
        const session = sessions.get(addr);

        let challenge;
        let verified = false;

        function packet(...args) {
            if (!ws || ws.readyState !== ws.OPEN) return;
            try {
                ws.send(pack(args));
            } catch (err) {
                if (err.code === 'EPIPE' || err.code === 'ERR_IPC_CHANNEL_CLOSED') {
                    console.warn(`WS send failed (${err.code}), client likely disconnected.`);
                } else {
                    console.error('WS send error:', err.code || err.message);
                }
            }
        }

        function safeWorkerSend(worker, message) {
            if (!worker || !worker.connected || worker.killed) return false;
            try {
                worker.send(message);
                return true;
            } catch (err) {
                // EPIPE / ERR_IPC_CHANNEL_CLOSED — common on Codespaces
                if (err.code === 'EPIPE' || err.code === 'ERR_IPC_CHANNEL_CLOSED' || err.code === 'ERR_IPC_DISCONNECTED') {
                    console.warn(`Worker IPC dead (${err.code}), removing worker.`);
                    session.workers = session.workers.filter(w => w !== worker);
                    try { worker.kill(); } catch (_) {}
                } else {
                    console.error("Worker send failed:", err.code || err.message);
                }
                return false;
            }
        }

        function close() {
            ws.close();
            // We only destroy workers if explicitly told to, or if the session is terminated.
            // For now, we don't destroy them on socket close to support refresh.
        }

        ws.on("message", (msg) => {
            try {
                const data = unpack(msg);
                const type = data.shift();

                switch (type) {
                    case "M":
                        if (challenge || data[0] != 72011) {
                            close();
                        }

                        challenge = randint(0b1000000000, 0b1111111111);
                        packet("M", challenge);
                        break;

                    case "C":
                        if (data[0] == (challenge ^ 845)) {
                            verified = true;
                            console.log(addr, "verified");
                        } else {
                            close();
                            console.log(addr, "true noob")
                        }

                        break;

                    case "Z":
                        session.tank = data[0];
                        if (session.tank instanceof Array) {
                            session.tanks = session.tank;
                            session.tankIdx = 0;

                            for (const worker of session.workers) {
                                if (worker.connected) {
                                    let t = session.tanks[session.tankIdx];
                                    safeWorkerSend(worker, { type: "tankselect", tank: t });

                                    session.tankIdx++;
                                    if (session.tankIdx >= session.tanks.length) {
                                        session.tankIdx = 0;
                                    }
                                }
                            }
                        } else {
                            session.tanks = [];
                            for (const worker of session.workers) {
                                if (worker.connected) safeWorkerSend(worker, { type: "tankselect", tank: session.tank });
                            }
                        }

                        break;

                    case "F":
                        if (verified) {
                            const hash = data[0];
                            const count = parseInt(data[1]) || 1;

                            console.log(`Spawning ${count} bots for hash: ${hash}`);

                            for (let i = 0; i < count; i++) {
                                setTimeout(() => {
                                    if (session.proxyIdx >= PROXIES.length) {
                                        session.proxyIdx = 0;
                                    }

                                    // Build the config for this worker (stored for re-fork on force respawn)
                                    let tankForWorker = session.tank;
                                    if (session.tanks.length) {
                                        tankForWorker = session.tanks[session.tankIdx];
                                        session.tankIdx++;
                                        if (session.tankIdx >= session.tanks.length) session.tankIdx = 0;
                                    }

                                    const workerConfig = {
                                        id: i,
                                        proxy: {
                                            type: "http",
                                            url: PROXIES[session.proxyIdx]
                                        },
                                        hash: "#" + hash,
                                        name: "",
                                        stats: [0, 0, 0, 0, 0, 0, 0, 9],
                                        type: "follow",
                                        token: "follow-8fe6ca",
                                        autoFire: false,
                                        autoRespawn: true,
                                        keys: [],
                                        keysHold: [],
                                        tank: "Auto4",
                                        chatSpam: "",
                                        squadId: hash,
                                        reconnectAttempts: 3,
                                        reconnectDelay: 10000,
                                    };

                                    const worker = forkWorker(session, ws, packet, workerConfig, tankForWorker);
                                    session.proxyIdx++;
                                }, i * 15); // 15ms delay between each bot spawn (fast but Codespace-safe)
                            }
                        }

                        break;

                    case "B":
                        if (verified) {
                            for (const worker of session.workers) {
                                safeWorkerSend(worker, { type: "destroy" });
                            }
                            session.workers = [];
                        }

                        break;

                    case "K":
                        if (verified) {
                            console.log(`Force respawn ALL: destroying ${session.workers.length} workers and re-forking...`);
                            const workersToRespawn = [...session.workers];
                            session.workers = [];

                            // Kill all existing workers
                            for (const worker of workersToRespawn) {
                                const savedConfig = worker._spawnConfig;
                                const savedTank = worker._spawnTank;
                                try { worker.kill(); } catch (_) {}

                                // Re-fork with same config after a tiny stagger
                                if (savedConfig) {
                                    setTimeout(() => {
                                        const newWorker = forkWorker(session, ws, packet, savedConfig, savedTank);
                                    }, Math.random() * 200); // Small stagger to avoid thundering herd
                                }
                            }
                        }
                        break;

                    case "A":
                        if (verified) {
                            for (const worker of session.workers) {
                                if (worker.connected) {
                                    safeWorkerSend(worker, {
                                        type: "position",
                                        x: data[0],
                                        y: data[1],
                                        mouseX: data[2],
                                        mouseY: data[3],
                                        mouseDown: data[4],
                                        rMouseDown: data[5],
                                        mouse: data[6],
                                        feeding: data[7],
                                        shift: data[8],
                                        autofire: data[9],
                                        autospin: data[10],
                                        manualMode: data[11],
                                        manualX: data[12],
                                        manualY: data[13],
                                        overrideMode: data[14],
                                        sneakMode: data[15]
                                    });
                                }
                            }
                        }
                        break;
                    case "S":
                        if (verified) {
                            session.swarmMode = data[0] || "normal";
                        }
                        break;
                    case "Y":
                        if (verified) {
                            const hash = data[0];
                            const count = parseInt(data[1]) || 1;
                            const time = Math.max(1, parseInt(data[2]) || 5);
                            const team = data[3] || null;

                            console.log(`Team scan request: ${hash} x${count} time=${time} team=${team}`);

                            // Prepare scan workers list
                            session.scanWorkers = session.scanWorkers || [];

                            for (let i = 0; i < count; i++) {
                                setTimeout(() => {
                                    if (session.proxyIdx >= PROXIES.length) session.proxyIdx = 0;

                                    let tankForWorker = session.tank;
                                    if (session.tanks.length) {
                                        tankForWorker = session.tanks[session.tankIdx];
                                        session.tankIdx++;
                                        if (session.tankIdx >= session.tanks.length) session.tankIdx = 0;
                                    }

                                    const scanConfig = {
                                        id: i,
                                        proxy: {
                                            type: "http",
                                            url: PROXIES[session.proxyIdx]
                                        },
                                        hash: "#" + hash,
                                        name: "[?>/7.]",
                                        stats: [0, 0, 0, 0, 0, 0, 0, 9],
                                        type: "scan",
                                        token: "scan-" + Date.now(),
                                        autoFire: false,
                                        autoRespawn: true,
                                        keys: [],
                                        keysHold: [],
                                        tank: "Auto4",
                                        chatSpam: "",
                                        squadId: hash,
                                        scanTeam: team,
                                        reconnectAttempts: 0,
                                        reconnectDelay: 0,
                                    };

                                    const worker = forkWorker(session, ws, packet, scanConfig, tankForWorker);
                                    if (!session.scanWorkers) session.scanWorkers = [];
                                    session.scanWorkers.push(worker);

                                    session.proxyIdx++;
                                }, i * 30); // 30ms delay between scan workers (fast but Codespace-safe)
                            }
                        }
                        break;

                    case "T":
                        if (verified) {
                            for (const worker of session.workers) {
                                if (worker.connected) {
                                    safeWorkerSend(worker, {
                                        type: "chat",
                                        message: data[0],
                                        spam: data[1]
                                    });
                                }
                            }
                        }
                        break;

                    default:
                        close();
                        break;
                }
            } catch (e) {
                console.error(e);
            }
        });

        ws.on("close", () => {
            console.log(addr, "disconnected (session retained)");
        });
    });


    const port = prod ? process.env.PORT : 8082;
    server.listen(port, () => {
        console.log("Server listening on port", port);
    });
})();
