import asyncio, json, socket, struct
from kademlia.network import Server  # aiokademlia

# 0.0.0.0 대신 127.0.0.1 사용 (로컬 테스트용)
# 실제 원격 부트스트랩 노드는 유지
BOOTSTRAP_NODES = [("127.0.0.1", 8468), ("dht.alpsoft.io", 8468)]


async def main():
    dht = Server()  # 1) DHT 시작
    await dht.listen(0)  #   0 → 랜덤포트

    # 자신의 주소 정보 가져오기
    # Server 객체는 server 속성이 없으므로 protocol 속성을 통해 접근
    local_ip = socket.gethostbyname(socket.gethostname())
    local_port = dht.protocol.transport.get_extra_info("sockname")[1]
    local_address = (local_ip, local_port)

    print(f"🌐 내 노드 정보: {local_ip}:{local_port}, ID: {dht.node.id.hex()}")

    try:
        # 부트스트랩에 타임아웃 적용
        await asyncio.wait_for(dht.bootstrap(BOOTSTRAP_NODES), timeout=10)
        print("✅ 부트스트랩 성공!")
    except asyncio.TimeoutError:
        print("⚠️ 부트스트랩 타임아웃 - 계속 진행합니다")
    except Exception as e:
        print(f"⚠️ 부트스트랩 오류: {e} - 계속 진행합니다")

    peer_id = dht.node.id.hex()

    # 3) 내 존재 알리기 - 수정된 주소 정보 사용
    try:
        await asyncio.wait_for(dht.set(peer_id, local_address), timeout=10)
        print(f"✅ DHT에 내 정보 저장 성공: {peer_id} -> {local_address}")
    except Exception as e:
        print(f"⚠️ DHT 저장 실패: {e}")

    # 4) 임의 토픽(예: "global-chat") 찾기
    topic = "global-chat"
    try:
        peers = await asyncio.wait_for(dht.get(topic), timeout=10)
        if not peers:
            peers = []

        # 리스트 타입인지 확인 (직렬화 문제 방지)
        if not isinstance(peers, list):
            print(f"⚠️ 예상치 못한 peers 형식: {type(peers)}, 빈 리스트로 초기화합니다")
            peers = []

        if local_address not in peers:
            peers.append(local_address)
            await asyncio.wait_for(dht.set(topic, peers), timeout=10)
            print(f"✅ 토픽에 참가 성공: {topic}")
    except Exception as e:
        print(f"⚠️ 토픽 참가 실패: {e}")
        peers = [local_address]  # 다른 피어를 찾지 못했을 경우 나만 있는 리스트로 초기화

    print("🌐 현재 토픽 참가자:", peers)

    # 5) 모든 동료와 직접 TCP 연결
    for host, port in peers:
        if (host, port) == local_address:
            continue
        try:
            print(f"🔄 연결 시도: {host}:{port}")
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=5
            )
            writer.write(b"HELLO FROM " + peer_id.encode())
            await writer.drain()
            print(f"✅ 연결 성공: {host}:{port}")

            # 응답 읽기 (선택적)
            response = await asyncio.wait_for(reader.read(100), timeout=5)
            print(f"📩 응답: {response.decode()}")

            # 연결 종료
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"❌ 연결 실패 {host}:{port}: {e}")

    # 6) 간단한 TCP 서버 시작 (다른 피어들의 연결을 받기 위해)
    async def handle_client(reader, writer):
        addr = writer.get_extra_info("peername")
        data = await reader.read(100)
        message = data.decode()
        print(f"📬 메시지 수신 ({addr[0]}:{addr[1]}): {message}")

        # 응답 보내기
        writer.write(f"ACK FROM {peer_id}".encode())
        await writer.drain()
        writer.close()

    # TCP 서버 시작
    server = await asyncio.start_server(handle_client, local_ip, local_port)

    print(f"🎧 TCP 서버 시작됨: {local_ip}:{local_port}")

    # 일정 시간 실행 후 종료 (테스트용, 실제로는 무한 실행이 필요할 수 있음)
    try:
        await asyncio.sleep(300)  # 5분 실행
    except asyncio.CancelledError:
        pass
    finally:
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
