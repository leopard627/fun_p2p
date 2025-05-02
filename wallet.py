"""
P2P Wallet implementation for funP2P
- Handles key generation
- Manages wallet state
- Signs and verifies transactions
"""

import os
import json
import base64
import hashlib
import time
from pathlib import Path
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Wallet:
    def __init__(self, wallet_path=None):
        """Initialize wallet with existing keys or generate new ones"""
        self.wallet_path = wallet_path or os.path.join(
            str(Path.home()), "funp2p_wallet.json"
        )
        self.address = None
        self.private_key = None
        self.public_key = None
        self.balance = 0
        self.transactions = []

        # Try to load existing wallet or create new one
        if os.path.exists(self.wallet_path):
            self._load_wallet()
        else:
            self._generate_new_wallet()
            self._save_wallet()

    def _generate_new_wallet(self):
        """Generate new Ed25519 keypair for the wallet"""
        private_key = ed25519.Ed25519PrivateKey.generate()
        public_key = private_key.public_key()

        # Serialize keys
        self.private_key = private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        self.public_key = public_key.public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        )

        # Generate address from public key (hash of public key)
        self.address = hashlib.sha256(self.public_key).hexdigest()[:40]
        logger.info(f"🔑 Generated new wallet with address: {self.address}")

    def _load_wallet(self):
        """Load wallet from file"""
        try:
            with open(self.wallet_path, "r") as f:
                wallet_data = json.load(f)

            self.address = wallet_data["address"]
            self.private_key = base64.b64decode(wallet_data["private_key"])
            self.public_key = base64.b64decode(wallet_data["public_key"])
            self.balance = wallet_data.get("balance", 0)
            self.transactions = wallet_data.get("transactions", [])
            logger.info(f"💼 Loaded wallet with address: {self.address}")
        except Exception as e:
            logger.error(f"❌ Failed to load wallet: {e}")
            self._generate_new_wallet()

    def _save_wallet(self):
        """Save wallet to file"""
        wallet_data = {
            "address": self.address,
            "private_key": base64.b64encode(self.private_key).decode("utf-8"),
            "public_key": base64.b64encode(self.public_key).decode("utf-8"),
            "balance": self.balance,
            "transactions": self.transactions,
        }

        # Ensure directory exists
        os.makedirs(os.path.dirname(self.wallet_path) or ".", exist_ok=True)

        try:
            with open(self.wallet_path, "w") as f:
                json.dump(wallet_data, f, indent=2)
            logger.info(f"💾 Wallet saved to {self.wallet_path}")
        except Exception as e:
            logger.error(f"❌ Failed to save wallet: {e}")

    def create_transaction(self, recipient, amount, memo=""):
        """Create and sign a transaction"""
        if amount <= 0:
            raise ValueError("Amount must be positive")

        if amount > self.balance:
            raise ValueError(f"Insufficient balance: {self.balance} < {amount}")

        # Create transaction data
        transaction = {
            "sender": self.address,
            "recipient": recipient,
            "amount": amount,
            "timestamp": int(time.time()),
            "memo": memo,
            "nonce": os.urandom(8).hex(),  # Random nonce to prevent replay
        }

        # Hash the transaction data
        tx_data = json.dumps(transaction, sort_keys=True).encode()
        tx_hash = hashlib.sha256(tx_data).digest()

        # Sign the transaction hash
        private_key_obj = ed25519.Ed25519PrivateKey.from_private_bytes(self.private_key)
        signature = private_key_obj.sign(tx_hash)

        # Add signature to transaction
        transaction["signature"] = base64.b64encode(signature).decode("utf-8")
        transaction["tx_hash"] = tx_hash.hex()

        # Update wallet state
        self.transactions.append(transaction)
        self.balance -= amount
        self._save_wallet()

        return transaction

    @staticmethod
    def verify_transaction(transaction):
        """Verify a transaction's signature"""
        try:
            # Recreate transaction data without signature for verification
            tx_copy = transaction.copy()
            signature = base64.b64decode(tx_copy.pop("signature"))
            tx_hash = bytes.fromhex(tx_copy.pop("tx_hash"))

            # Get sender's public key from the DHT or local cache (implementation needed)
            # This is a placeholder - you'll need to fetch the actual public key
            public_key_bytes = b""  # Placeholder

            # Verify signature
            public_key = ed25519.Ed25519PublicKey.from_public_bytes(public_key_bytes)
            public_key.verify(signature, tx_hash)
            return True
        except Exception as e:
            logger.error(f"❌ Transaction verification failed: {e}")
            return False

    def get_balance(self):
        """Get current wallet balance"""
        return self.balance

    def update_balance(self, new_balance):
        """Update wallet balance (after synchronizing with network)"""
        self.balance = new_balance
        self._save_wallet()
        logger.info(f"💰 Wallet balance updated: {self.balance}")
