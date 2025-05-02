import asyncio, json, socket, struct, time, os
from kademlia_wrapper import SafeKademliaServer  # 새로운 래퍼 클래스 사용
import logging
import random
from wallet import Wallet
from wallet_dht import WalletDHT

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# 환경 변수에서 설정 가져오기
bootstrap_node = os.environ.get("BOOTSTRAP_NODE", "127.0.0.1:8468")
bootstrap_host, bootstrap_port = bootstrap_node.split(":")
bootstrap_port = int(bootstrap_port)

# 타임아웃 설정 가져오기
RPC_TIMEOUT = int(os.environ.get("RPC_TIMEOUT", "10"))  # 기본값 10초로 증가
BOOTSTRAP_RETRY = int(os.environ.get("BOOTSTRAP_RETRY", "5"))  # 재시도 횟수 증가

# 부트스트랩 노드 설정
BOOTSTRAP_NODES = [(bootstrap_host, bootstrap_port)]
logger.info(f"부트스트랩 노드 설정: {BOOTSTRAP_NODES}")

# 갱신 주기 (초) - 환경 변수에서 가져오기
DHT_REFRESH_INTERVAL = int(os.environ.get("DHT_REFRESH_INTERVAL", "60"))
PEER_HEARTBEAT_INTERVAL = int(
    os.environ.get("PEER_HEARTBEAT_INTERVAL", "180")
)  # 기본값 3분으로 증가
PEER_TIMEOUT = int(os.environ.get("PEER_TIMEOUT", "600"))  # 기본값 10분으로 증가


async def main():
    logger.info("P2P 노드 시작 중...")
    # 예외 처리가 개선된 SafeKademliaServer 사용
    dht = SafeKademliaServer()
    await dht.listen(0)  # 0 → 랜덤포트

    # 자신의 주소 정보 가져오기
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except:
        # 호스트 이름 확인 실패 시 대체 방법 사용
        local_ip = "127.0.0.1"
        try:
            # 도커 환경에서 컨테이너 IP 가져오기 시도
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                local_ip = s.getsockname()[0]
        except:
            logger.warning(
                "⚠️ 로컬 IP 주소를 가져올 수 없습니다. 127.0.0.1을 사용합니다."
            )

    local_port = dht.protocol.transport.get_extra_info("sockname")[1]
    local_address = (local_ip, local_port)

    logger.info(f"🌐 내 노드 정보: {local_ip}:{local_port}, ID: {dht.node.id.hex()}")

    # 노드 시작 시 약간의 지연 추가 - 컨테이너 간 시작 타이밍 분산
    start_delay = random.uniform(1, 5)
    logger.info(f"부트스트랩 시작 전 {start_delay:.1f}초 대기...")
    await asyncio.sleep(start_delay)

    # 2) 부트스트랩 노드에 연결 - 여러 번 재시도
    bootstrap_success = await dht.bootstrap(
        BOOTSTRAP_NODES, retry_count=BOOTSTRAP_RETRY, retry_delay=2
    )
    if bootstrap_success:
        logger.info("✅ 부트스트랩 성공!")
    else:
        logger.warning("⚠️ 부트스트랩 실패 - 계속 진행합니다")

    # 노드 ID를 16진수 문자열로 변환
    peer_id = dht.node.id.hex()

    # 3) 내 존재 알리기 - 주소를 JSON으로 직렬화
    try:
        # 타임스탬프를 포함한 피어 정보
        peer_info = {
            "ip": local_ip,
            "port": local_port,
            "last_seen": time.time(),  # 타임스탬프 추가
        }
        address_json = json.dumps(peer_info)
        success = await dht.safe_set(peer_id, address_json, max_attempts=3, delay=2)
        if success:
            logger.info(f"✅ DHT에 내 정보 저장 성공: {peer_id}")
        else:
            logger.warning(f"⚠️ DHT에 내 정보 저장 실패")
    except Exception as e:
        logger.warning(f"⚠️ DHT 저장 실패: {e}")

    # 피어 상태 캐시 - 피어 ID를 키로, 마지막 응답 시간을 값으로
    peer_status_cache = {}

    # 새로운 부분: 지갑 초기화
    wallet = Wallet()
    wallet_dht = WalletDHT(dht, wallet)

    # 지갑을 네트워크에 등록
    try:
        await wallet_dht.register_wallet(announce=False)  # 기본적으로 공개하지 않음
        logger.info(f"💰 지갑 초기화 완료: {wallet.address}")
    except Exception as e:
        logger.warning(f"⚠️ 지갑 등록 실패: {e}")

    # 4) 임의 토픽(예: "global-chat") 찾기
    topic = "global-chat"
    peers = []

    try:
        # 토픽에 저장된 피어 목록 가져오기
        topic_data = await dht.safe_get(topic, max_attempts=3, delay=2)

        if topic_data:
            try:
                # JSON 문자열로 저장된 피어 목록 파싱
                peers = json.loads(topic_data)
                logger.info(f"✅ 토픽 '{topic}'에서 {len(peers)} 피어 발견")
            except json.JSONDecodeError:
                logger.warning(f"⚠️ 토픽 데이터 형식 오류: {topic_data}")
                peers = []

        # 내 정보 추가 - 타임스탬프 포함
        my_peer_info = {
            "id": peer_id,
            "ip": local_ip,
            "port": local_port,
            "last_seen": time.time(),  # 타임스탬프 추가
            "wallet": wallet.address,  # 지갑 주소 추가
        }

        # 기존 내 정보가 있으면 업데이트, 없으면 추가
        updated = False
        for i, peer in enumerate(peers):
            if peer.get("id") == peer_id:
                peers[i] = my_peer_info
                updated = True
                break

        if not updated:
            peers.append(my_peer_info)

        # 갱신된 피어 목록 저장
        peers_json = json.dumps(peers)
        success = await dht.safe_set(topic, peers_json, max_attempts=3, delay=2)
        if success:
            logger.info(f"✅ 토픽 '{topic}'에 참가 성공")
        else:
            logger.warning(f"⚠️ 토픽 참가 실패")

    except Exception as e:
        logger.warning(f"⚠️ 토픽 참가 실패: {e}")
        # 실패 시 나만 있는 목록으로 초기화
        peers = [my_peer_info]

    logger.info(f"🌐 현재 토픽 참가자: {len(peers)}명")

    # 5) TCP 서버 시작 (다른 피어들의 연결을 받기 위해)
    async def handle_client(reader, writer):
        addr = writer.get_extra_info("peername")
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=RPC_TIMEOUT)
            message = data.decode()
            logger.info(f"📬 메시지 수신 ({addr[0]}:{addr[1]}): {message[:50]}...")

            # 메시지 처리 - PING 요청이면 PONG으로 응답
            if message.startswith("PING"):
                try:
                    # PING 메시지에서 피어 ID 추출
                    sender_id = message.split()[1]
                    # 피어 상태 업데이트
                    peer_status_cache[sender_id] = time.time()
                    response = f"PONG {peer_id}"
                except:
                    response = f"PONG {peer_id}"
            # 트랜잭션 메시지 처리 추가
            elif message.startswith("TX"):
                try:
                    # 트랜잭션 데이터 파싱
                    tx_data = json.loads(message[3:])
                    # 트랜잭션 검증 (실제 구현에서는 더 철저하게 검증해야 함)
                    if Wallet.verify_transaction(tx_data):
                        # 트랜잭션 브로드캐스트
                        await wallet_dht.broadcast_transaction(tx_data)
                        response = f"TX_ACK {tx_data['tx_hash']}"
                    else:
                        response = "TX_INVALID"
                except Exception as e:
                    logger.error(f"트랜잭션 처리 오류: {e}")
                    response = "TX_ERROR"
            else:
                # 일반 메시지인 경우
                response = f"ACK FROM {peer_id[:10]}..."

            writer.write(response.encode())
            await writer.drain()
        except asyncio.TimeoutError:
            logger.warning(f"⚠️ 클라이언트 요청 타임아웃 ({addr[0]}:{addr[1]})")
        except ConnectionResetError:
            logger.warning(f"⚠️ 연결 재설정 ({addr[0]}:{addr[1]})")
        except Exception as e:
            logger.error(f"❌ 클라이언트 처리 오류 ({addr[0]}:{addr[1]}): {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass

    # TCP 서버 시작
    server = None
    max_retries = 5
    for retry in range(max_retries):
        try:
            server = await asyncio.start_server(handle_client, local_ip, local_port)
            logger.info(f"🎧 TCP 서버 시작됨: {local_ip}:{local_port}")
            break
        except Exception as e:
            if retry < max_retries - 1:
                logger.warning(
                    f"⚠️ TCP 서버 시작 실패, 재시도 중 ({retry+1}/{max_retries}): {e}"
                )
                await asyncio.sleep(2)
            else:
                logger.error(f"❌ TCP 서버 시작 최대 재시도 횟수 초과: {e}")
                # 대체 포트 시도
                try:
                    alt_port = random.randint(10000, 65000)
                    server = await asyncio.start_server(
                        handle_client, local_ip, alt_port
                    )
                    local_port = alt_port
                    logger.info(
                        f"🎧 대체 포트로 TCP 서버 시작됨: {local_ip}:{local_port}"
                    )
                except Exception as e2:
                    logger.error(f"❌ 대체 포트로도 TCP 서버 시작 실패: {e2}")
                    return

    if not server:
        logger.error("❌ TCP 서버를 시작할 수 없습니다.")
        return

    # 6) 피어 하트비트 체크 - 피어의 활성 상태 검증
    async def check_peer_heartbeat():
        while True:
            await asyncio.sleep(PEER_HEARTBEAT_INTERVAL)
            logger.info("💓 피어 하트비트 체크 중...")

            # 현재 알려진 모든 피어에 대해 하트비트 체크
            try:
                # 토픽에서 최신 피어 목록 가져오기
                topic_data = await dht.safe_get(topic, max_attempts=2)
                if not topic_data:
                    continue

                try:
                    current_peers = json.loads(topic_data)
                except json.JSONDecodeError:
                    logger.warning("⚠️ 피어 목록 형식 오류, 건너뜁니다")
                    continue

                active_peers = []

                # 각 피어에 대해 PING 테스트
                for peer in current_peers:
                    peer_id = peer.get("id")
                    if not peer_id:
                        continue

                    # 자기 자신은 건너뛰기
                    if peer_id == peer_id:
                        active_peers.append(peer)
                        continue

                    host = peer.get("ip")
                    port = peer.get("port")
                    last_seen = peer.get("last_seen", 0)

                    if not host or not port:
                        continue

                    # 최근에 이미 확인된 피어는 다시 체크하지 않음
                    current_time = time.time()
                    if (
                        peer_id in peer_status_cache
                        and current_time - peer_status_cache[peer_id] < PEER_TIMEOUT
                    ):
                        # 캐시된 상태 정보 사용
                        peer["last_seen"] = peer_status_cache[peer_id]
                        active_peers.append(peer)
                        continue

                    # 피어가 최근에 업데이트되었다면 활성으로 간주
                    if current_time - last_seen < PEER_TIMEOUT:
                        active_peers.append(peer)
                        continue

                    # PING 메시지 전송하여 활성 여부 확인
                    try:
                        logger.info(f"🔄 PING 전송: {host}:{port}")
                        reader, writer = await asyncio.wait_for(
                            asyncio.open_connection(host, port), timeout=5
                        )

                        # PING 메시지 전송
                        ping_message = f"PING {peer_id}"
                        writer.write(ping_message.encode())
                        await writer.drain()

                        # 응답 읽기
                        response = await asyncio.wait_for(reader.read(100), timeout=5)
                        response_str = response.decode()

                        if response_str.startswith("PONG"):
                            # 활성 피어로 간주
                            logger.info(f"✅ 활성 피어 확인: {host}:{port}")
                            peer["last_seen"] = time.time()
                            peer_status_cache[peer_id] = time.time()
                            active_peers.append(peer)
                        else:
                            logger.warning(f"⚠️ 예상치 못한 응답: {response_str}")

                        # 연결 종료
                        writer.close()
                        await writer.wait_closed()
                    except Exception as e:
                        logger.warning(f"❌ 피어 접속 실패 {host}:{port}")
                        # 최근에 추가된 피어라면 한 번의 실패는 용서
                        if current_time - last_seen < PEER_TIMEOUT / 2:
                            active_peers.append(peer)

                # 피어 목록 업데이트
                if len(active_peers) != len(current_peers):
                    # 내 정보 확실히 포함시키기
                    my_peer_info = {
                        "id": peer_id,
                        "ip": local_ip,
                        "port": local_port,
                        "last_seen": time.time(),
                        "wallet": wallet.address,  # 지갑 주소 추가
                    }

                    # 내 정보가 없으면 추가
                    if not any(p.get("id") == peer_id for p in active_peers):
                        active_peers.append(my_peer_info)

                    # DHT 업데이트
                    peers_json = json.dumps(active_peers)
                    success = await dht.safe_set(topic, peers_json, max_attempts=2)
                    if success:
                        logger.info(
                            f"✅ 정리된 피어 목록 업데이트: {len(current_peers)} → {len(active_peers)}"
                        )
                    else:
                        logger.warning("⚠️ 피어 목록 업데이트 실패")

            except Exception as e:
                logger.warning(f"⚠️ 하트비트 체크 실패: {str(e)[:100]}...")

    # 7) 피어 목록 갱신 및 내 정보 유지
    async def refresh_peers():
        while True:
            try:
                await asyncio.sleep(DHT_REFRESH_INTERVAL)
                logger.info("🔄 피어 목록 갱신 중...")

                # 내 정보 갱신
                peer_info = {
                    "ip": local_ip,
                    "port": local_port,
                    "last_seen": time.time(),
                }
                address_json = json.dumps(peer_info)
                success = await dht.safe_set(peer_id, address_json, max_attempts=2)
                if success:
                    logger.info("✅ DHT에 내 정보 갱신됨")
                else:
                    logger.warning("⚠️ DHT에 내 정보 갱신 실패")

                # 토픽에서 최신 피어 목록 가져오기
                topic_data = await dht.safe_get(topic, max_attempts=2)
                if topic_data:
                    try:
                        peers = json.loads(topic_data)
                        logger.info(f"✅ 토픽에서 {len(peers)} 피어 발견")

                        # 내 정보 업데이트
                        my_info_updated = False
                        for i, peer in enumerate(peers):
                            if peer.get("id") == peer_id:
                                peers[i] = {
                                    "id": peer_id,
                                    "ip": local_ip,
                                    "port": local_port,
                                    "last_seen": time.time(),
                                    "wallet": wallet.address,  # 지갑 주소 추가
                                }
                                my_info_updated = True
                                break

                        # 내 정보가 없으면 추가
                        if not my_info_updated:
                            peers.append(
                                {
                                    "id": peer_id,
                                    "ip": local_ip,
                                    "port": local_port,
                                    "last_seen": time.time(),
                                    "wallet": wallet.address,  # 지갑 주소 추가
                                }
                            )

                        # 갱신된 피어 목록 저장
                        peers_json = json.dumps(peers)
                        success = await dht.safe_set(topic, peers_json, max_attempts=2)
                        if success:
                            logger.info("✅ 토픽 피어 목록 갱신됨")
                        else:
                            logger.warning("⚠️ 토픽 피어 목록 갱신 실패")
                    except json.JSONDecodeError:
                        logger.warning(f"⚠️ 토픽 데이터 형식 오류: {topic_data[:50]}...")

                # 간헐적으로 랜덤 피어 연결 테스트 (지나친 트래픽 방지를 위해 20% 확률로만)
                if random.random() < 0.2 and peers:
                    random_peer = random.choice(peers)
                    if random_peer.get("id") != peer_id:  # 자기 자신이 아닌지 확인
                        host = random_peer.get("ip")
                        port = random_peer.get("port")
                        if host and port:
                            try:
                                logger.info(f"🔄 랜덤 피어 연결 테스트: {host}:{port}")
                                reader, writer = await asyncio.wait_for(
                                    asyncio.open_connection(host, port), timeout=5
                                )
                                # 간단한 핑 메시지
                                writer.write(f"PING {peer_id}".encode())
                                await writer.drain()
                                # 응답은 무시
                                writer.close()
                                await writer.wait_closed()
                                logger.info(f"✅ 랜덤 피어 연결 성공: {host}:{port}")
                            except Exception as e:
                                logger.warning(
                                    f"❌ 랜덤 피어 연결 실패: {str(e)[:100]}..."
                                )
            except Exception as e:
                logger.warning(f"⚠️ 피어 목록 갱신 실패: {str(e)[:100]}...")

    # 8) DHT 갱신 - K-버킷을 최신 상태로 유지
    async def refresh_dht():
        while True:
            try:
                # 4시간마다 전체 DHT 리프레시
                await asyncio.sleep(4 * 60 * 60)
                logger.info("🔄 DHT 버킷 리프레시 시작...")

                # K-버킷 갱신
                success = await dht.bootstrap(
                    BOOTSTRAP_NODES, retry_count=3, retry_delay=2
                )
                if success:
                    logger.info("✅ DHT 버킷 리프레시 성공")
                else:
                    logger.warning("⚠️ DHT 버킷 리프레시 실패")

                # 임의 키 쿼리로 DHT 상태 유지
                random_key = str(random.getrandbits(160))
                await dht.safe_get(random_key, max_attempts=1)

                logger.info("✅ DHT 버킷 리프레시 완료")
            except Exception as e:
                logger.warning(f"⚠️ DHT 리프레시 실패: {str(e)[:100]}...")

    # 9) 지갑 동기화 태스크 추가
    async def wallet_sync_task():
        while True:
            try:
                await asyncio.sleep(60)  # 1분마다 동기화
                await wallet_dht.sync_wallet()
                logger.info(f"💰 지갑 동기화 완료. 잔액: {wallet.balance}")
            except Exception as e:
                logger.warning(f"⚠️ 지갑 동기화 실패: {str(e)[:100]}...")

    # 10) 오류 복구 태스크 - 연결 끊김 감지 및 재연결
    async def reconnect_task():
        consecutive_failures = 0
        max_failures = 5  # 연속 실패 최대 허용 수

        while True:
            await asyncio.sleep(5 * 60)  # 5분마다 체크

            try:
                # 간단한 DHT 테스트 쿼리 수행
                test_key = "dht:ping"
                test_value = f"ping:{int(time.time())}"
                success = await dht.safe_set(test_key, test_value, max_attempts=1)

                if success:
                    # 성공하면 카운터 리셋
                    if consecutive_failures > 0:
                        logger.info(
                            f"✅ DHT 연결 복구됨 (이전 실패: {consecutive_failures}회)"
                        )
                    consecutive_failures = 0
                else:
                    # 실패 카운터 증가
                    consecutive_failures += 1
                    logger.warning(
                        f"⚠️ DHT 테스트 실패 ({consecutive_failures}/{max_failures})"
                    )

                    if consecutive_failures >= max_failures:
                        # 최대 실패 횟수 초과 시 재연결 시도
                        logger.warning("🔄 DHT 재연결 시도 중...")

                        # 부트스트랩 노드에 다시 연결
                        success = await dht.bootstrap(
                            BOOTSTRAP_NODES, retry_count=3, retry_delay=2
                        )
                        if success:
                            logger.info("✅ DHT 재연결 성공")
                            consecutive_failures = 0
                        else:
                            logger.error("❌ DHT 재연결 실패")
            except Exception as e:
                consecutive_failures += 1
                logger.warning(f"⚠️ 연결 복구 검사 중 오류: {str(e)[:100]}...")

    # 태스크 시작
    refresh_peer_task = asyncio.create_task(refresh_peers())
    heartbeat_task = asyncio.create_task(check_peer_heartbeat())
    dht_refresh_task = asyncio.create_task(refresh_dht())
    wallet_task = asyncio.create_task(wallet_sync_task())
    reconnect_task = asyncio.create_task(reconnect_task())  # 새 태스크 추가

    # 서버 실행 유지
    try:
        async with server:
            stop_event = asyncio.Event()

            # 종료 신호 처리
            def handle_exit():
                logger.info("종료 신호 수신...")
                stop_event.set()

            # Ctrl+C 처리
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop = asyncio.get_running_loop()
                    loop.add_signal_handler(sig, handle_exit)
                except (NotImplementedError, RuntimeError):
                    # Windows에서는 add_signal_handler가 작동하지 않음
                    pass

            # 메인 루프 - 종료 신호 대기
            await stop_event.wait()
            logger.info("안전한 종료 시작...")
    except asyncio.CancelledError:
        logger.info("🛑 서버 종료 중...")
    except Exception as e:
        logger.error(f"❌ 서버 실행 중 오류: {str(e)[:100]}...")
    finally:
        # 모든 태스크 취소
        for task in [
            refresh_peer_task,
            heartbeat_task,
            dht_refresh_task,
            wallet_task,
            reconnect_task,
        ]:
            task.cancel()

        try:
            # 안전하게 종료
            pending = [
                refresh_peer_task,
                heartbeat_task,
                dht_refresh_task,
                wallet_task,
                reconnect_task,
            ]
            await asyncio.gather(*pending, return_exceptions=True)

            # 마지막으로 내 상태 업데이트 - 오프라인 표시
            try:
                # 토픽에서 최신 피어 목록 가져오기
                topic_data = await dht.safe_get(topic, max_attempts=1)
                if topic_data:
                    peers = json.loads(topic_data)

                    # 내 정보 제거
                    peers = [p for p in peers if p.get("id") != peer_id]

                    # 업데이트된 목록 저장
                    peers_json = json.dumps(peers)
                    await dht.safe_set(topic, peers_json, max_attempts=1)
                    logger.info("✅ 피어 목록에서 내 정보 제거됨")
            except:
                pass

            logger.info("👋 모든 자원이 안전하게 종료됨")
        except asyncio.CancelledError:
            pass


# 모듈이 직접 실행될 때
if __name__ == "__main__":
    # 누락된 signal 모듈 import 추가
    import signal

    try:
        # 예외 처리 개선
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("키보드 인터럽트로 종료됨")
        except Exception as e:
            logger.error(f"예기치 않은 오류로 종료됨: {str(e)[:100]}...")
    finally:
        # 정상 종료 확인
        logger.info("프로그램 종료")
