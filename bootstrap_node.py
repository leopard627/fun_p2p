"""
Single-process Kademlia bootstrap node.
• persists DHT data every 60 s
• optional seed list ⇒ 노드 간 메시에 직접 연결
"""

import asyncio, os, pickle, signal, logging
from kademlia.network import Server

LISTEN_IP = "0.0.0.0"
LISTEN_PORT = 8468
DATA_FILE = "/var/lib/dht_store.pkl"  # 볼륨 마운트 또는 호스트 경로
SEED_NODES = [("dht1.example.com", 8468), ("dht2.example.com", 8468)]  # 다른 부트스트랩 노드

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    server = Server()  # K=20, alpha=3 by default
    await server.listen(LISTEN_PORT, LISTEN_IP)

    # 1) 이전 스토어 로드
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "rb") as fp:
                server.storage.data = pickle.load(fp)
            logging.info("🎒  persisted DHT loaded, %d keys", len(server.storage.data))
        except Exception as e:
            logging.warning("Could not load store: %s", e)

    # 2) 다른 부트스트랩 노드에 자동 커넥트 (순환 연결)
    # if SEED_NODES:
    #     try:
    #         await server.bootstrap(SEED_NODES)
    #         logging.info("🔗  connected to %d seed nodes", len(SEED_NODES))
    #     except Exception as e:
    #         logging.warning("bootstrap() failed: %s", e)
    #
    # 3) 주기적 스냅숏
    async def persist():
        while True:
            await asyncio.sleep(60)
            with open(DATA_FILE, "wb") as fp:
                pickle.dump(server.storage.data, fp)

    asyncio.create_task(persist())

    # 4) graceful shutdown
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_event_loop().add_signal_handler(sig, stop.set)
    await stop.wait()


if __name__ == "__main__":
    asyncio.run(main())
