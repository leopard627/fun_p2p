"""
Single-process Kademlia bootstrap node.
• persists DHT data every 60 s
• optional seed list ⇒ 노드 간 메시에 직접 연결
"""

import asyncio, os, pickle, signal, logging, json
from kademlia.network import Server
from pathlib import Path

LISTEN_IP = "0.0.0.0"  # 모든 인터페이스에서 수신
LISTEN_PORT = 8468
# 사용자 홈 디렉토리에 저장 - 권한 문제 해결
DATA_FILE = os.path.join(str(Path.home()), "dht_store.pkl")
SEED_NODES = [("dht1.example.com", 8468), ("dht2.example.com", 8468)]  # 다른 부트스트랩 노드

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    server = Server()  # K=20, alpha=3 by default
    await server.listen(LISTEN_PORT, LISTEN_IP)
    logger.info(f"🌟 부트스트랩 노드 시작됨: {LISTEN_IP}:{LISTEN_PORT}")
    logger.info(f"🔑 노드 ID: {server.node.id.hex()}")

    # 1) 이전 스토어 로드
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "rb") as fp:
                server.storage.data = pickle.load(fp)
            logger.info("🎒 persisted DHT loaded, %d keys", len(server.storage.data))

            # 저장된 키 목록 출력 (디버깅용)
            logger.info("현재 저장된 키 목록:")
            for key in server.storage.data.keys():
                try:
                    # 키가 바이너리 형태일 수 있음
                    key_str = key.hex() if isinstance(key, bytes) else str(key)
                    value = server.storage.data[key]
                    value_str = str(value)[:100]  # 값이 길면 자름
                    logger.info(f"  - {key_str}: {value_str}")
                except Exception as e:
                    logger.warning(f"키 표시 중 오류: {e}")
        except Exception as e:
            logger.warning("Could not load store: %s", e)

    # 2) 다른 부트스트랩 노드에 자동 커넥트 (순환 연결)
    # if SEED_NODES:
    #     try:
    #         await server.bootstrap(SEED_NODES)
    #         logger.info("🔗  connected to %d seed nodes", len(SEED_NODES))
    #     except Exception as e:
    #         logger.warning("bootstrap() failed: %s", e)
    #
    # 3) 주기적 스냅숏
    async def persist():
        while True:
            try:
                await asyncio.sleep(60)
                # 저장 전 디버깅 정보
                logger.info(f"💾 DHT 데이터 저장 중... ({len(server.storage.data)} 키)")

                # 디렉토리가 없으면 생성
                os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)

                with open(DATA_FILE, "wb") as fp:
                    pickle.dump(server.storage.data, fp)
                logger.info(f"✅ DHT 데이터 저장 완료: {DATA_FILE}")
            except Exception as e:
                logger.error(f"❌ 데이터 저장 실패: {e}")

    persist_task = asyncio.create_task(persist())

    # 4) graceful shutdown
    stop = asyncio.Event()

    def handle_signal():
        logger.info("🛑 종료 신호 받음, 정리 중...")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_event_loop().add_signal_handler(sig, handle_signal)

    await stop.wait()

    # 종료 전 마지막 저장
    try:
        logger.info("💾 종료 전 DHT 데이터 저장 중...")
        with open(DATA_FILE, "wb") as fp:
            pickle.dump(server.storage.data, fp)
        logger.info("👋 종료 완료!")
    except Exception as e:
        logger.error(f"❌ 종료 시 데이터 저장 실패: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("키보드 인터럽트로 종료됨")
    except Exception as e:
        logger.error(f"예기치 않은 오류로 종료됨: {e}")
