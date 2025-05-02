"""
DHT Wallet Integration for funP2P
- Stores wallet addresses and public keys in DHT
- Broadcasts transactions through the network
- Syncs wallet state from network
"""

import json
import time
import asyncio
import logging
from wallet import Wallet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WalletDHT:
    def __init__(self, dht_client, wallet=None):
        """Initialize wallet DHT integration"""
        self.dht = dht_client  # Your existing Kademlia DHT client
        self.wallet = wallet or Wallet()
        self.peers = {}
        self.pending_transactions = []
        self.confirmed_transactions = []

    async def register_wallet(self, announce=False):
        """
        지갑 주소와 공개 키를 DHT에 등록합니다.
        announce: True면 글로벌 디렉토리에도 추가, False면 필요시에만 검색 가능
        """
        try:
            # 지갑 정보 객체 생성
            wallet_info = {
                "address": self.wallet.address,
                "public_key": self.wallet.public_key.hex(),
                "last_seen": time.time(),
            }

            # DHT에 지갑 주소 키로 저장 - 이건 유지
            key = f"wallet:{self.wallet.address}"
            await self.dht.set(key, json.dumps(wallet_info))
            logger.info(f"✅ 지갑 DHT에 등록됨: {self.wallet.address}")

            # 선택적: 글로벌 디렉토리 업데이트
            if announce:
                await self._update_wallet_directory()
                logger.info("✅ 지갑이 글로벌 디렉토리에 공개됨")

            return True
        except Exception as e:
            logger.error(f"❌ 지갑 등록 실패: {e}")
            return False

    async def _update_wallet_directory(self):
        """Update the directory of all wallets in the network"""
        try:
            # Get existing wallet directory
            wallets_key = "global:wallets"
            wallets_data = await self.dht.get(wallets_key)

            if wallets_data:
                wallets = json.loads(wallets_data)
            else:
                wallets = []

            # Check if our wallet is already in directory
            addresses = [w["address"] for w in wallets]
            if self.wallet.address not in addresses:
                # Add our wallet
                wallet_entry = {
                    "address": self.wallet.address,
                    "last_seen": time.time(),
                }
                wallets.append(wallet_entry)

                # Update directory in DHT
                await self.dht.set(wallets_key, json.dumps(wallets))
                logger.info("✅ Added wallet to global directory")

            return True
        except Exception as e:
            logger.error(f"❌ Failed to update wallet directory: {e}")
            return False

    async def get_wallet_info(self, address):
        """Retrieve wallet info from DHT"""
        try:
            key = f"wallet:{address}"
            wallet_data = await self.dht.get(key)

            if wallet_data:
                return json.loads(wallet_data)
            else:
                logger.warning(f"⚠️ Wallet not found: {address}")
                return None
        except Exception as e:
            logger.error(f"❌ Failed to get wallet info: {e}")
            return None

    async def broadcast_transaction(self, transaction):
        """Broadcast a transaction to the network"""
        try:
            # Add transaction to pending pool
            tx_key = f"tx:{transaction['tx_hash']}"
            await self.dht.set(tx_key, json.dumps(transaction))

            # Also update the transaction pool
            await self._update_transaction_pool(transaction)

            logger.info(f"📣 Transaction broadcast: {transaction['tx_hash']}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to broadcast transaction: {e}")
            return False

    async def _update_transaction_pool(self, transaction):
        """Update the global transaction pool"""
        try:
            # Get existing transaction pool
            pool_key = "global:tx_pool"
            pool_data = await self.dht.get(pool_key)

            if pool_data:
                tx_pool = json.loads(pool_data)
            else:
                tx_pool = []

            # Add new transaction
            tx_pool.append(
                {
                    "tx_hash": transaction["tx_hash"],
                    "sender": transaction["sender"],
                    "recipient": transaction["recipient"],
                    "amount": transaction["amount"],
                    "timestamp": transaction["timestamp"],
                }
            )

            # Keep only recent transactions (last 100)
            tx_pool = sorted(tx_pool, key=lambda x: x["timestamp"], reverse=True)[:100]

            # Update pool in DHT
            await self.dht.set(pool_key, json.dumps(tx_pool))
            return True
        except Exception as e:
            logger.error(f"❌ Failed to update transaction pool: {e}")
            return False

    async def sync_wallet(self):
        """Sync wallet state with the network"""
        try:
            # Get transaction pool
            pool_key = "global:tx_pool"
            pool_data = await self.dht.get(pool_key)

            if not pool_data:
                logger.info("ℹ️ No transaction pool found")
                return False

            tx_pool = json.loads(pool_data)

            # Find transactions involving our wallet
            my_transactions = [
                tx
                for tx in tx_pool
                if tx["sender"] == self.wallet.address
                or tx["recipient"] == self.wallet.address
            ]

            # Calculate balance
            incoming = sum(
                tx["amount"]
                for tx in my_transactions
                if tx["recipient"] == self.wallet.address
            )
            outgoing = sum(
                tx["amount"]
                for tx in my_transactions
                if tx["sender"] == self.wallet.address
            )

            # Update wallet balance
            new_balance = incoming - outgoing
            self.wallet.update_balance(new_balance)

            logger.info(f"🔄 Wallet synced with network. New balance: {new_balance}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to sync wallet: {e}")
            return False

    async def get_wallet_directory(self):
        """Get list of all wallets in the network"""
        try:
            wallets_key = "global:wallets"
            wallets_data = await self.dht.get(wallets_key)

            if wallets_data:
                return json.loads(wallets_data)
            else:
                return []
        except Exception as e:
            logger.error(f"❌ Failed to get wallet directory: {e}")
            return []
