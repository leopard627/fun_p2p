import asyncio, json, socket, struct
from kademlia.network import Server  # aiokademlia

BOOTSTRAP_NODES = [("0.0.0.0", 8468), ("dht.alpsoft.io", 8468)]


async def main():
    dht = Server()  # 1) DHT 시작
    await dht.listen(0)  #   0 → 랜덤포트
    await dht.bootstrap(BOOTSTRAP_NODES)  # 2) 부트스트랩
    peer_id = dht.node.id.hex()

    # 3) 내 존재 알리기
    await dht.set(peer_id, dht.server.address)

    # 4) 임의 토픽(예: “global-chat”) 찾기
    topic = "global-chat"
    peers = await dht.get(topic) or []
    if dht.server.address not in peers:
        peers.append(dht.server.address)
        await dht.set(topic, peers)

    print("🌐 현재 토픽 참가자:", peers)

    # 5) 모든 동료와 직접 TCP 연결
    for host, port in peers:
        if (host, port) == dht.server.address:
            continue
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(b"HELLO FROM " + peer_id.encode())
            await writer.drain()
            print("✔ connected to", host, port)
        except Exception as e:
            print("✗", host, port, e)


asyncio.run(main())
