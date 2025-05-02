"""
Single-process Kademlia bootstrap node.
- persists DHT data every 60 s
- cleans up stale DHT entries every 30 minutes
- optional seed list ⇒ 노드 간 메시에 직접 연결
"""

import asyncio, os, pickle, signal, logging, json, time
from kademlia_wrapper import SafeKademliaServer  # 안전한 래퍼 사용
from pathlib import Path

# 환경 변수에서 설정 가져오기
LISTEN_IP = os.environ.get("LISTEN_IP", "0.0.0.0")  # 모든 인터페이스에서 수신
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8468"))
RPC_TIMEOUT = int(os.environ.get("RPC_TIMEOUT", "10"))  # 타임아웃 값 증가

# 사용자 홈 디렉토리에 저장 - 권한 문제 해결
DATA_FILE = os.path.join(str(Path.home()), "dht_store.pkl")
SEED_NODES = [
    ("dht1.example.com", 8468),
    ("dht2.example.com", 8468),
]  # 다른 부트스트랩 노드
# 키 만료 시간 (초) - 3시간
KEY_EXPIRY_TIME = int(os.environ.get("KEY_EXPIRY_TIME", str(3 * 60 * 60)))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


async def main() -> None:
    # 안전한 DHT 서버 사용
    server = SafeKademliaServer()  # K=20, alpha=3 by default
    await server.listen(LISTEN_PORT, LISTEN_IP)
    logger.info(f"🌟 부트스트랩 노드 시작됨: {LISTEN_IP}:{LISTEN_PORT}")
    logger.info(f"🔑 노드 ID: {server.node.id.hex()}")

    # 1) 이전 스토어 로드 및 타임스탬프 정보 초기화
    timestamp_data = {}

    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "rb") as fp:
                server.storage.data = pickle.load(fp)
            logger.info("🎒 persisted DHT loaded, %d keys", len(server.storage.data))

            # 타임스탬프 데이터 초기화 - 기존 데이터는 현재 시간으로 설정
            current_time = time.time()
            for key in server.storage.data.keys():
                timestamp_data[key] = current_time

            # 저장된 키 목록 출력 (디버깅용)
            logger.info("현재 저장된 키 목록:")
            key_count = 0
            for key in server.storage.data.keys():
                try:
                    # 키가 바이너리 형태일 수 있음
                    key_str = key.hex() if isinstance(key, bytes) else str(key)
                    value = server.storage.data[key]
                    value_str = (
                        str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                    )
                    key_count += 1
                    if key_count <= 10:  # 최대 10개만 로그에 출력
                        logger.info(f"  - {key_str}: {value_str}")
                except Exception as e:
                    logger.warning(f"키 표시 중 오류: {e}")

            if key_count > 10:
                logger.info(f"  ... 외 {key_count - 10}개 키")
        except Exception as e:
            logger.warning("Could not load store: %s", e)
            # 로드 실패 시 빈 데이터로 초기화
            server.storage.data = {}
            timestamp_data = {}

    # 사용자 정의 storage provider - 타임스탬프 관리를 위해 기존 storage 메서드 래핑
    original_set_item = server.storage.__setitem__
    original_get_item = server.storage.__getitem__
    original_iter = server.storage.__iter__

    # Kademlia의 ForgetfulStorage에는 __contains__가 없을 수 있음
    # 직접 in 연산자 검사 대신 데이터 사전 체크

    # SET 오버라이드 - 키 설정 시 타임스탬프 업데이트
    def set_with_timestamp(key, value):
        timestamp_data[key] = time.time()
        return original_set_item(key, value)

    # GET 오버라이드 - 키 접근 시 타임스탬프 업데이트
    def get_with_timestamp(key):
        if key in timestamp_data:
            timestamp_data[key] = time.time()
        return original_get_item(key)

    # 타임스탬프 래핑 적용
    server.storage.__setitem__ = set_with_timestamp
    server.storage.__getitem__ = get_with_timestamp
    server.storage.__iter__ = original_iter

    # 2) 다른 부트스트랩 노드에 자동 커넥트 (순환 연결)
    # 환경 변수에서 시드 노드 목록을 가져올 수 있음
    seed_node_env = os.environ.get("SEED_NODES", "")
    if seed_node_env:
        try:
            # 형식: "host1:port1,host2:port2"
            seed_nodes = []
            for node in seed_node_env.split(","):
                if ":" in node:
                    host, port = node.split(":")
                    seed_nodes.append((host, int(port)))

            if seed_nodes:
                try:
                    result = await server.bootstrap(
                        seed_nodes, retry_count=3, retry_delay=2
                    )
                    if result:
                        logger.info(f"🔗 connected to {len(seed_nodes)} seed nodes")
                    else:
                        logger.warning(f"⚠️ bootstrap() to seed nodes failed")
                except Exception as e:
                    logger.warning(f"bootstrap() failed: {e}")
        except Exception as e:
            logger.warning(f"Could not parse SEED_NODES: {e}")

    # 3) 주기적 스냅숏
    async def persist():
        while True:
            try:
                await asyncio.sleep(60)
                # 저장 전 디버깅 정보
                logger.info(f"💾 DHT 데이터 저장 중... ({len(server.storage.data)} 키)")

                # 디렉토리가 없으면 생성
                os.makedirs(os.path.dirname(DATA_FILE) or ".", exist_ok=True)

                try:
                    with open(DATA_FILE, "wb") as fp:
                        pickle.dump(server.storage.data, fp)
                    logger.info(f"✅ DHT 데이터 저장 완료: {DATA_FILE}")
                except Exception as e:
                    logger.error(f"❌ 데이터 저장 실패: {e}")

                    # 백업 경로에 저장 시도
                    backup_file = DATA_FILE + ".backup"
                    try:
                        with open(backup_file, "wb") as fp:
                            pickle.dump(server.storage.data, fp)
                        logger.info(f"✅ DHT 데이터 백업에 저장됨: {backup_file}")
                    except Exception as e2:
                        logger.error(f"❌ 백업 데이터 저장도 실패: {e2}")
            except Exception as e:
                logger.error(f"❌ 데이터 저장 실패: {e}")

    persist_task = asyncio.create_task(persist())

    # 4) DHT 정리 로직 - 오래된 키 제거
    async def cleanup_dht():
        while True:
            try:
                # 30분마다 실행
                await asyncio.sleep(30 * 60)
                logger.info("🧹 DHT 정리 작업 시작...")

                current_time = time.time()
                keys_to_remove = []

                # 만료된 키 찾기
                for key, last_access in list(timestamp_data.items()):
                    if current_time - last_access > KEY_EXPIRY_TIME:
                        try:
                            # 토픽 키인 경우 특별 처리 - 내용을 검증
                            if isinstance(key, str) and "global-" in str(key):
                                try:
                                    # 토픽 피어 목록 가져와서 정리
                                    topic_data = server.storage.data.get(key)
                                    if topic_data:
                                        try:
                                            peers = json.loads(topic_data)
                                            # 여기서 더 정교한 검증 로직 추가 가능
                                            if peers and isinstance(peers, list):
                                                # 토픽은 유지하고 타임스탬프만 업데이트
                                                timestamp_data[key] = current_time
                                                continue
                                        except json.JSONDecodeError:
                                            # JSON 파싱 실패 시 제거 대상으로
                                            pass
                                except:
                                    pass  # 처리 실패 시 제거 대상으로

                            # "wallet:" 키 접두사가 있는 키도 특별 처리
                            if isinstance(key, str) and str(key).startswith("wallet:"):
                                try:
                                    # 지갑 정보 유효성 확인
                                    wallet_data = server.storage.data.get(key)
                                    if wallet_data:
                                        wallet_info = json.loads(wallet_data)
                                        if wallet_info and "address" in wallet_info:
                                            # 지갑 정보는 더 오래 유지
                                            timestamp_data[key] = current_time
                                            continue
                                except:
                                    pass

                            # 피어 ID인 경우 - 일반 피어 정보
                            keys_to_remove.append(key)
                            # str 대신 repr 사용하여 안전하게 로깅
                            key_repr = repr(key)
                            logger.info(f"🗑️ 만료된 키 제거 예정: {key_repr}")
                        except Exception as e:
                            logger.warning(f"키 처리 중 오류: {e}")

                # 만료된 키 제거
                for key in keys_to_remove:
                    try:
                        if (
                            key in server.storage.data.keys()
                        ):  # in 연산자 대신 keys() 메서드 사용
                            del server.storage.data[key]
                        if key in timestamp_data:
                            del timestamp_data[key]
                    except Exception as e:
                        logger.warning(f"키 제거 중 오류: {e}")

                logger.info(
                    f"✅ DHT 정리 완료. {len(keys_to_remove)}개 키 제거됨. 남은 키: {len(server.storage.data)}개"
                )

                # 피어 목록 정리 - 특별 케이스 처리
                try:
                    key = "global-chat"
                    if (
                        key in server.storage.data.keys()
                    ):  # in 연산자 대신 keys() 메서드 사용
                        topic_data = server.storage.data[key]
                        peers = json.loads(topic_data)

                        # 피어 활성화 검증 (여기서는 단순화)
                        current_time = time.time()
                        valid_peers = []
                        for peer in peers:
                            peer_id = peer.get("id")
                            last_seen = peer.get("last_seen", 0)

                            # 최근에 활동한 피어만 유지
                            if current_time - last_seen < KEY_EXPIRY_TIME:
                                valid_peers.append(peer)
                            else:
                                logger.info(f"🗑️ 오래된 피어 제거: {peer_id}")

                        # 정리된 피어 목록 저장
                        if len(valid_peers) != len(peers):
                            peers_json = json.dumps(valid_peers)
                            server.storage.data[key] = peers_json
                            timestamp_data[key] = current_time
                            logger.info(
                                f"🔄 토픽 '{key}' 피어 목록 정리됨: {len(peers)} → {len(valid_peers)}"
                            )
                except Exception as e:
                    logger.warning(f"토픽 정리 중 오류: {e}")

            except Exception as e:
                logger.error(f"❌ DHT 정리 실패: {e}")

    cleanup_task = asyncio.create_task(cleanup_dht())

    # 5) 주기적 상태 보고
    async def status_report():
        while True:
            try:
                await asyncio.sleep(15 * 60)  # 15분마다 상태 보고

                # 기본 노드 정보
                logger.info(f"📊 부트스트랩 노드 상태 보고")
                logger.info(f"  - 노드 ID: {server.node.id.hex()}")
                logger.info(f"  - 저장된 키: {len(server.storage.data)}개")

                # K-버킷 상태
                node_count = 0
                for i, bucket in enumerate(server.protocol.router.buckets):
                    node_count += len(bucket.nodes)
                logger.info(f"  - 라우팅 테이블: {node_count}개 노드")

                # 글로벌 채팅 토픽 피어 수
                try:
                    if "global-chat" in server.storage.data:
                        chat_data = server.storage.data["global-chat"]
                        peers = json.loads(chat_data)
                        logger.info(f"  - 글로벌 채팅 참가자: {len(peers)}명")
                except:
                    pass

                # 지갑 통계
                wallet_count = 0
                for key in server.storage.data.keys():
                    if isinstance(key, str) and str(key).startswith("wallet:"):
                        wallet_count += 1
                logger.info(f"  - 등록된 지갑: {wallet_count}개")

            except Exception as e:
                logger.warning(f"⚠️ 상태 보고 실패: {e}")

    status_task = asyncio.create_task(status_report())

    # 6) graceful shutdown
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

    # 태스크 취소
    cleanup_task.cancel()
    persist_task.cancel()
    status_task.cancel()
    try:
        await asyncio.gather(
            cleanup_task, persist_task, status_task, return_exceptions=True
        )
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("키보드 인터럽트로 종료됨")
    except Exception as e:
        logger.error(f"예기치 않은 오류로 종료됨: {e}")
