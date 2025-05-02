"""
DHT 연결 테스트 - 노드 간 통신이 제대로 이루어지는지 확인합니다.
"""

import asyncio
import logging
import json
import sys
import os
import time
import uuid
from kademlia.network import Server

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

# 테스트 설정
BOOTSTRAP_HOST = os.environ.get("BOOTSTRAP_HOST", "127.0.0.1")
BOOTSTRAP_PORT = int(os.environ.get("BOOTSTRAP_PORT", 8468))
TEST_VALUE = str(uuid.uuid4())  # 임의의 고유한 테스트 값 생성
TEST_KEY = "test_key_" + str(int(time.time()))  # 시간 기반 테스트 키


async def run_test():
    """DHT 연결 및 키-값 저장/검색 테스트"""
    logger.info("DHT 연결 테스트 시작...")
    logger.info(f"부트스트랩 노드: {BOOTSTRAP_HOST}:{BOOTSTRAP_PORT}")
    logger.info(f"테스트 키: {TEST_KEY}")
    logger.info(f"테스트 값: {TEST_VALUE}")

    # 노드 1 설정 - 데이터 저장용
    node1 = Server()
    await node1.listen(0)  # 랜덤 포트 사용
    node1_port = node1.protocol.transport.get_extra_info("sockname")[1]
    logger.info(f"노드 1 (저장 노드) 시작됨: 127.0.0.1:{node1_port}")

    # 부트스트랩 노드에 연결
    try:
        await asyncio.wait_for(
            node1.bootstrap([(BOOTSTRAP_HOST, BOOTSTRAP_PORT)]), timeout=5
        )
        logger.info("✅ 노드 1의 부트스트랩 연결 성공")
    except Exception as e:
        logger.error(f"❌ 노드 1의 부트스트랩 연결 실패: {e}")
        return False

    # 데이터 저장
    try:
        await asyncio.wait_for(node1.set(TEST_KEY, TEST_VALUE), timeout=5)
        logger.info(f"✅ 노드 1이 데이터 저장 성공: {TEST_KEY} -> {TEST_VALUE}")
    except Exception as e:
        logger.error(f"❌ 노드 1의 데이터 저장 실패: {e}")
        return False

    # 노드 2 설정 - 데이터 검색용
    node2 = Server()
    await node2.listen(0)  # 랜덤 포트 사용
    node2_port = node2.protocol.transport.get_extra_info("sockname")[1]
    logger.info(f"노드 2 (조회 노드) 시작됨: 127.0.0.1:{node2_port}")

    # 부트스트랩 노드에 연결
    try:
        await asyncio.wait_for(
            node2.bootstrap([(BOOTSTRAP_HOST, BOOTSTRAP_PORT)]), timeout=5
        )
        logger.info("✅ 노드 2의 부트스트랩 연결 성공")
    except Exception as e:
        logger.error(f"❌ 노드 2의 부트스트랩 연결 실패: {e}")
        return False

    # 데이터가 DHT에 전파되는 데 약간의 시간이 필요할 수 있음
    await asyncio.sleep(2)

    # 데이터 검색
    try:
        result = await asyncio.wait_for(node2.get(TEST_KEY), timeout=5)
        logger.info(f"✅ 노드 2가 데이터 검색 결과: {result}")

        if result == TEST_VALUE:
            logger.info("✅ 테스트 성공: 저장된 값과 검색된 값이 일치함")
            return True
        else:
            logger.error(
                f"❌ 테스트 실패: 값이 일치하지 않음 (예상: {TEST_VALUE}, 실제: {result})"
            )
            return False
    except Exception as e:
        logger.error(f"❌ 노드 2의 데이터 검색 실패: {e}")
        return False
    finally:
        # 노드 종료
        node1.stop()
        node2.stop()


if __name__ == "__main__":
    try:
        result = asyncio.run(run_test())
        if result:
            logger.info("🎉 DHT 연결 테스트 성공")
            sys.exit(0)
        else:
            logger.error("❌ DHT 연결 테스트 실패")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("키보드 인터럽트로 종료됨")
        sys.exit(1)
    except Exception as e:
        logger.error(f"예기치 않은 오류로 테스트 실패: {e}")
        sys.exit(1)
