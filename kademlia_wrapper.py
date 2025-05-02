# kademlia_wrapper.py
from kademlia.network import Server
import asyncio
import logging

logger = logging.getLogger(__name__)


class SafeKademliaServer(Server):
    """Kademlia 서버에 안전 장치를 추가한 래퍼 클래스"""

    def __init__(self, ksize=20, alpha=3, node_id=None, storage=None):
        super().__init__(ksize, alpha, node_id, storage)

    async def bootstrap(self, addrs, retry_count=3, retry_delay=1):
        """부트스트랩 메서드에 재시도 로직 추가"""
        for attempt in range(retry_count):
            try:
                result = await asyncio.wait_for(super().bootstrap(addrs), timeout=10)
                return result
            except asyncio.TimeoutError:
                if attempt < retry_count - 1:
                    logger.warning(
                        f"⚠️ 부트스트랩 타임아웃. {retry_delay}초 후 재시도 ({attempt+1}/{retry_count})..."
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(
                        f"❌ 부트스트랩 최대 재시도 횟수 초과 ({retry_count}회)"
                    )
                    return False
            except Exception as e:
                logger.error(f"❌ 부트스트랩 중 오류 발생: {e}")
                if attempt < retry_count - 1:
                    logger.warning(
                        f"⚠️ {retry_delay}초 후 재시도 ({attempt+1}/{retry_count})..."
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"❌ 부트스트랩 재시도 실패")
                    return False

    async def safe_get(self, key, max_attempts=3, delay=1):
        """get 메서드에 재시도 로직과 예외 처리 추가"""
        for attempt in range(max_attempts):
            try:
                result = await asyncio.wait_for(super().get(key), timeout=5)
                return result
            except asyncio.TimeoutError:
                if attempt < max_attempts - 1:
                    logger.warning(
                        f"⚠️ 값 조회 타임아웃 ({key}). {delay}초 후 재시도 ({attempt+1}/{max_attempts})..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"❌ 값 조회 최대 재시도 횟수 초과 ({max_attempts}회)")
                    return None
            except Exception as e:
                logger.warning(f"⚠️ 값 조회 중 오류 발생 ({key}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"❌ 값 조회 재시도 실패")
                    return None

    async def safe_set(self, key, value, max_attempts=3, delay=1):
        """set 메서드에 재시도 로직과 예외 처리 추가"""
        for attempt in range(max_attempts):
            try:
                result = await asyncio.wait_for(super().set(key, value), timeout=5)
                return result
            except asyncio.TimeoutError:
                if attempt < max_attempts - 1:
                    logger.warning(
                        f"⚠️ 값 저장 타임아웃 ({key}). {delay}초 후 재시도 ({attempt+1}/{max_attempts})..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"❌ 값 저장 최대 재시도 횟수 초과 ({max_attempts}회)")
                    return False
            except Exception as e:
                logger.warning(f"⚠️ 값 저장 중 오류 발생 ({key}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"❌ 값 저장 재시도 실패")
                    return False
