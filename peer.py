import asyncio, json, socket, struct
from kademlia.network import Server  # aiokademlia
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# 로컬 부트스트랩 노드와 외부 노드 모두 사용
BOOTSTRAP_NODES = [("127.0.0.1", 8468), ("dht.alpsoft.io", 8468)]


async def main():
    logger.info("P2P 노드 시작 중...")
    dht = Server()  # 1) DHT 시작
    await dht.listen(0)  #   0 → 랜덤포트

    # 자신의 주소 정보 가져오기
    local_ip = socket.gethostbyname(socket.gethostname())
    local_port = dht.protocol.transport.get_extra_info("sockname")[1]
    local_address = (local_ip, local_port)

    logger.info(f"🌐 내 노드 정보: {local_ip}:{local_port}, ID: {dht.node.id.hex()}")

    # 2) 부트스트랩 노드에 연결
    try:
        # 부트스트랩에 타임아웃 적용
        await asyncio.wait_for(dht.bootstrap(BOOTSTRAP_NODES), timeout=10)
        logger.info("✅ 부트스트랩 성공!")
    except asyncio.TimeoutError:
        logger.warning("⚠️ 부트스트랩 타임아웃 - 계속 진행합니다")
    except Exception as e:
        logger.warning(f"⚠️ 부트스트랩 오류: {e} - 계속 진행합니다")

    peer_id = dht.node.id.hex()

    # 3) 내 존재 알리기 - 주소를 JSON으로 직렬화
    try:
        # 튜플은 직렬화할 수 없으므로 JSON 문자열로 변환
        address_json = json.dumps({"ip": local_ip, "port": local_port})
        await asyncio.wait_for(dht.set(peer_id, address_json), timeout=10)
        logger.info(f"✅ DHT에 내 정보 저장 성공: {peer_id} -> {address_json}")
    except Exception as e:
        logger.warning(f"⚠️ DHT 저장 실패: {e}")

    # 4) 임의 토픽(예: "global-chat") 찾기
    topic = "global-chat"
    peers = []

    try:
        # 토픽에 저장된 피어 목록 가져오기
        topic_data = await asyncio.wait_for(dht.get(topic), timeout=10)

        if topic_data:
            try:
                # JSON 문자열로 저장된 피어 목록 파싱
                peers = json.loads(topic_data)
                logger.info(f"✅ 토픽 '{topic}'에서 {len(peers)} 피어 발견")
            except json.JSONDecodeError:
                logger.warning(f"⚠️ 토픽 데이터 형식 오류: {topic_data}")
                peers = []

        # 내 정보 추가
        my_peer_info = {"id": peer_id, "ip": local_ip, "port": local_port}
        if not any(p.get("id") == peer_id for p in peers):
            peers.append(my_peer_info)

        # 갱신된 피어 목록 저장
        peers_json = json.dumps(peers)
        await asyncio.wait_for(dht.set(topic, peers_json), timeout=10)
        logger.info(f"✅ 토픽 '{topic}'에 참가 성공")

    except Exception as e:
        logger.warning(f"⚠️ 토픽 참가 실패: {e}")
        # 실패 시 나만 있는 목록으로 초기화
        peers = [{"id": peer_id, "ip": local_ip, "port": local_port}]

    logger.info(f"🌐 현재 토픽 참가자: {json.dumps(peers, indent=2)}")

    # 5) 모든 동료와 직접 TCP 연결
    for peer in peers:
        # 자기 자신은 건너뛰기
        if peer.get("id") == peer_id:
            continue

        host = peer.get("ip")
        port = peer.get("port")

        if not host or not port:
            logger.warning(f"⚠️ 피어 정보 형식 오류: {peer}")
            continue

        try:
            logger.info(f"🔄 연결 시도: {host}:{port}")
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=5
            )

            # 메시지 전송
            message = f"HELLO FROM {peer_id}"
            writer.write(message.encode())
            await writer.drain()
            logger.info(f"✅ 연결 성공: {host}:{port}")

            # 응답 읽기
            try:
                response = await asyncio.wait_for(reader.read(100), timeout=5)
                logger.info(f"📩 응답: {response.decode()}")
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ 응답 타임아웃: {host}:{port}")

            # 연결 종료
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            logger.warning(f"❌ 연결 실패 {host}:{port}: {e}")

    # 6) TCP 서버 시작 (다른 피어들의 연결을 받기 위해)
    async def handle_client(reader, writer):
        addr = writer.get_extra_info("peername")
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=5)
            message = data.decode()
            logger.info(f"📬 메시지 수신 ({addr[0]}:{addr[1]}): {message}")

            # 응답 보내기
            response = f"ACK FROM {peer_id}"
            writer.write(response.encode())
            await writer.drain()
        except Exception as e:
            logger.error(f"❌ 클라이언트 처리 오류 ({addr[0]}:{addr[1]}): {e}")
        finally:
            writer.close()

    # TCP 서버 시작
    server = await asyncio.start_server(handle_client, local_ip, local_port)

    logger.info(f"🎧 TCP 서버 시작됨: {local_ip}:{local_port}")

    # 60초마다 토픽 피어 목록 갱신
    async def refresh_peers():
        while True:
            try:
                await asyncio.sleep(60)
                logger.info("🔄 피어 목록 갱신 중...")

                # 토픽에서 최신 피어 목록 가져오기
                topic_data = await asyncio.wait_for(dht.get(topic), timeout=10)
                if topic_data:
                    peers = json.loads(topic_data)
                    logger.info(f"✅ 토픽에서 {len(peers)} 피어 발견")

                    # 내 정보가 없으면 추가
                    if not any(p.get("id") == peer_id for p in peers):
                        peers.append(
                            {"id": peer_id, "ip": local_ip, "port": local_port}
                        )
                        peers_json = json.dumps(peers)
                        await asyncio.wait_for(dht.set(topic, peers_json), timeout=10)
                        logger.info("✅ 피어 목록에 내 정보 추가됨")
            except Exception as e:
                logger.warning(f"⚠️ 피어 목록 갱신 실패: {e}")

    # 피어 갱신 태스크 시작
    refresh_task = asyncio.create_task(refresh_peers())

    # 서버 실행 유지
    async with server:
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            logger.info("🛑 서버 종료 중...")
        finally:
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("키보드 인터럽트로 종료됨")
    except Exception as e:
        logger.error(f"예기치 않은 오류로 종료됨: {e}")
