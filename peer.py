import asyncio, json, socket, struct, time
from kademlia.network import Server  # aiokademlia
import logging
import random

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# 로컬 부트스트랩 노드와 외부 노드 모두 사용
BOOTSTRAP_NODES = [("127.0.0.1", 8468), ("dht.alpsoft.io", 8468)]

# 갱신 주기 (초)
DHT_REFRESH_INTERVAL = 60  # 피어 정보 갱신 주기
PEER_HEARTBEAT_INTERVAL = 120  # 피어 하트비트 체크 주기
PEER_TIMEOUT = 300  # 5분간 응답 없으면 피어 제거


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
        # 타임스탬프를 포함한 피어 정보
        peer_info = {
            "ip": local_ip, 
            "port": local_port,
            "last_seen": time.time()  # 타임스탬프 추가
        }
        address_json = json.dumps(peer_info)
        await asyncio.wait_for(dht.set(peer_id, address_json), timeout=10)
        logger.info(f"✅ DHT에 내 정보 저장 성공: {peer_id} -> {address_json}")
    except Exception as e:
        logger.warning(f"⚠️ DHT 저장 실패: {e}")

    # 피어 상태 캐시 - 피어 ID를 키로, 마지막 응답 시간을 값으로
    peer_status_cache = {}

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

        # 내 정보 추가 - 타임스탬프 포함
        my_peer_info = {
            "id": peer_id, 
            "ip": local_ip, 
            "port": local_port,
            "last_seen": time.time()  # 타임스탬프 추가
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
        await asyncio.wait_for(dht.set(topic, peers_json), timeout=10)
        logger.info(f"✅ 토픽 '{topic}'에 참가 성공")

    except Exception as e:
        logger.warning(f"⚠️ 토픽 참가 실패: {e}")
        # 실패 시 나만 있는 목록으로 초기화
        peers = [my_peer_info]

    logger.info(f"🌐 현재 토픽 참가자: {json.dumps(peers, indent=2)}")

    # 5) TCP 서버 시작 (다른 피어들의 연결을 받기 위해)
    async def handle_client(reader, writer):
        addr = writer.get_extra_info("peername")
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=5)
            message = data.decode()
            logger.info(f"📬 메시지 수신 ({addr[0]}:{addr[1]}): {message}")

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
            else:
                # 일반 메시지인 경우
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

    # 6) 피어 하트비트 체크 - 피어의 활성 상태 검증
    async def check_peer_heartbeat():
        while True:
            await asyncio.sleep(PEER_HEARTBEAT_INTERVAL)
            logger.info("💓 피어 하트비트 체크 중...")
            
            # 현재 알려진 모든 피어에 대해 하트비트 체크
            try:
                # 토픽에서 최신 피어 목록 가져오기
                topic_data = await asyncio.wait_for(dht.get(topic), timeout=10)
                if not topic_data:
                    continue
                    
                current_peers = json.loads(topic_data)
                active_peers = []
                
                # 각 피어에 대해 PING 테스트
                for peer in current_peers:
                    peer_id = peer.get("id")
                    
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
                    if peer_id in peer_status_cache and current_time - peer_status_cache[peer_id] < PEER_TIMEOUT:
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
                        logger.warning(f"❌ 피어 접속 실패 {host}:{port}: {e}")
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
                        "last_seen": time.time()
                    }
                    
                    # 내 정보가 없으면 추가
                    if not any(p.get("id") == peer_id for p in active_peers):
                        active_peers.append(my_peer_info)
                    
                    # DHT 업데이트
                    peers_json = json.dumps(active_peers)
                    await asyncio.wait_for(dht.set(topic, peers_json), timeout=10)
                    logger.info(f"✅ 정리된 피어 목록 업데이트: {len(current_peers)} → {len(active_peers)}")
                
            except Exception as e:
                logger.warning(f"⚠️ 하트비트 체크 실패: {e}")
    
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
                    "last_seen": time.time()
                }
                address_json = json.dumps(peer_info)
                await asyncio.wait_for(dht.set(peer_id, address_json), timeout=10)
                logger.info("✅ DHT에 내 정보 갱신됨")

                # 토픽에서 최신 피어 목록 가져오기
                topic_data = await asyncio.wait_for(dht.get(topic), timeout=10)
                if topic_data:
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
                                "last_seen": time.time()
                            }
                            my_info_updated = True
                            break
                    
                    # 내 정보가 없으면 추가
                    if not my_info_updated:
                        peers.append({
                            "id": peer_id, 
                            "ip": local_ip, 
                            "port": local_port,
                            "last_seen": time.time()
                        })
                    
                    # 갱신된 피어 목록 저장
                    peers_json = json.dumps(peers)
                    await asyncio.wait_for(dht.set(topic, peers_json), timeout=10)
                    logger.info("✅ 토픽 피어 목록 갱신됨")
                    
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
                                    logger.info(f"✅ 랜덤 피어 연결 성공: {host}:{port}")
                                except Exception as e:
                                    logger.warning(f"❌ 랜덤 피어 연결 실패: {e}")
            except Exception as e:
                logger.warning(f"⚠️ 피어 목록 갱신 실패: {e}")

    # 8) DHT 갱신 - K-버킷을 최신 상태로 유지
    async def refresh_dht():
        while True:
            try:
                # 4시간마다 전체 DHT 리프레시
                await asyncio.sleep(4 * 60 * 60)
                logger.info("🔄 DHT 버킷 리프레시 시작...")
                
                # K-버킷 갱신
                await dht.bootstrap(BOOTSTRAP_NODES)
                
                # 임의 키 쿼리로 DHT 상태 유지
                random_key = str(random.getrandbits(160))
                await dht.get(random_key)
                
                logger.info("✅ DHT 버킷 리프레시 완료")
            except Exception as e:
                logger.warning(f"⚠️ DHT 리프레시 실패: {e}")

    # 태스크 시작
    refresh_peer_task = asyncio.create_task(refresh_peers())
    heartbeat_task = asyncio.create_task(check_peer_heartbeat())
    dht_refresh_task = asyncio.create_task(refresh_dht())

    # 서버 실행 유지
    async with server:
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            logger.info("🛑 서버 종료 중...")
        finally:
            # 모든 태스크 취소
            refresh_peer_task.cancel()
            heartbeat_task.cancel()
            dht_refresh_task.cancel()
            try:
                await asyncio.gather(
                    refresh_peer_task, 
                    heartbeat_task, 
                    dht_refresh_task, 
                    return_exceptions=True
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
