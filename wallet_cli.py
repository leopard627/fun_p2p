"""
Command line interface for funP2P wallet
- Create wallet
- Check balance
- Send transactions
- List network wallets
- Import/export wallet
- Generate funds (for testing)
"""

import asyncio
import argparse
import json
import logging
import base64
import datetime
import os
from pathlib import Path
from kademlia.network import Server
from wallet import Wallet
from wallet_dht import WalletDHT

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

async def main():
    parser = argparse.ArgumentParser(description="funP2P Wallet CLI")
    parser.add_argument('--bootstrap', default='127.0.0.1:8468', help='Bootstrap node address:port')
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # Create new wallet
    create_parser = subparsers.add_parser('create', help='Create a new wallet')
    create_parser.add_argument('--announce', action='store_true', help='Announce wallet to the network')
    
    # Get balance
    balance_parser = subparsers.add_parser('balance', help='Check wallet balance')
    
    # Send transaction
    send_parser = subparsers.add_parser('send', help='Send a transaction')
    send_parser.add_argument('recipient', help='Recipient wallet address')
    send_parser.add_argument('amount', type=float, help='Amount to send')
    send_parser.add_argument('--memo', default='', help='Transaction memo')
    
    # List wallets
    list_parser = subparsers.add_parser('list', help='List known wallets')
    
    # Backup wallet
    backup_parser = subparsers.add_parser('backup', help='Backup wallet private key')
    backup_parser.add_argument('--file', default='wallet_backup.txt', help='Backup file path')
    
    # Import wallet
    import_parser = subparsers.add_parser('import', help='Import wallet from backup')
    import_parser.add_argument('file', help='Backup file path')
    
    # Get wallet address
    address_parser = subparsers.add_parser('address', help='Show wallet address')
    
    # Generate test funds (for development only)
    fund_parser = subparsers.add_parser('fund', help='Generate test funds (DEVELOPMENT ONLY)')
    fund_parser.add_argument('amount', type=float, help='Amount to generate')
    
    # Transaction history
    history_parser = subparsers.add_parser('history', help='Show transaction history')
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Special case for address command - doesn't need network connection
    if args.command == 'address':
        wallet = Wallet()
        logger.info(f"Wallet Address: {wallet.address}")
        return
    
    # Initialize DHT client
    bootstrap_host, bootstrap_port = args.bootstrap.split(':')
    bootstrap_port = int(bootstrap_port)
    
    dht = Server()
    await dht.listen(0)  # Use random port
    
    # Try to bootstrap
    try:
        await dht.bootstrap([(bootstrap_host, bootstrap_port)])
        logger.info(f"✅ Connected to bootstrap node: {bootstrap_host}:{bootstrap_port}")
    except Exception as e:
        logger.error(f"❌ Failed to connect to bootstrap node: {e}")
        return
    
    # Import command - handle before initializing wallet
    if args.command == 'import':
        try:
            with open(args.file, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    if line.startswith("Wallet Address:"):
                        address = line.split(":")[1].strip()
                    if line.startswith("Private Key:"):
                        private_key = base64.b64decode(line.split(":")[1].strip())
            
            # Create a new wallet file with the imported data
            wallet_path = os.path.join(str(Path.home()), "funp2p_wallet.json")
            wallet_data = {
                'address': address,
                'private_key': base64.b64encode(private_key).decode('utf-8'),
                'balance': 0,
                'transactions': []
            }
            
            with open(wallet_path, 'w') as f:
                json.dump(wallet_data, f, indent=2)
                
            logger.info(f"✅ Wallet imported: {address}")
            return
        except Exception as e:
            logger.error(f"❌ Failed to import wallet: {e}")
            return
    
    # Initialize wallet
    wallet = Wallet()
    wallet_dht = WalletDHT(dht, wallet)
    
    # Process command
    if args.command == 'create':
        # Creating a wallet happens automatically on init
        # Optionally announce to network
        if args.announce:
            await wallet_dht.register_wallet(announce=True)
            logger.info(f"✅ Created and announced new wallet: {wallet.address}")
        else:
            await wallet_dht.register_wallet(announce=False)
            logger.info(f"✅ Created new wallet: {wallet.address}")
            logger.info("Note: Wallet was not announced to the network")
        
        logger.info(f"💰 Initial balance: {wallet.balance}")
    
    elif args.command == 'balance':
        # Sync wallet first
        await wallet_dht.sync_wallet()
        logger.info(f"💰 Current balance: {wallet.balance}")
    
    elif args.command == 'send':
        try:
            # Sync wallet first
            await wallet_dht.sync_wallet()
            
            # Check if we have enough funds
            if wallet.balance < args.amount:
                logger.error(f"❌ Insufficient balance: {wallet.balance} < {args.amount}")
                return
            
            # Create and broadcast transaction
            tx = wallet.create_transaction(args.recipient, args.amount, args.memo)
            success = await wallet_dht.broadcast_transaction(tx)
            
            if success:
                logger.info(f"✅ Transaction sent: {tx['tx_hash']}")
                logger.info(f"  From: {wallet.address}")
                logger.info(f"  To: {args.recipient}")
                logger.info(f"  Amount: {args.amount}")
                if args.memo:
                    logger.info(f"  Memo: {args.memo}")
                logger.info(f"💰 New balance: {wallet.balance}")
            else:
                logger.error("❌ Failed to send transaction")
        except Exception as e:
            logger.error(f"❌ Transaction error: {e}")
    
    elif args.command == 'list':
        wallets = await wallet_dht.get_wallet_directory()
        if wallets:
            logger.info(f"Found {len(wallets)} wallets in the network:")
            for w in wallets:
                last_seen = time.time() - w.get('last_seen', 0)
                last_seen_str = f"{int(last_seen)}s ago" if last_seen < 3600 else f"{int(last_seen/3600)}h ago"
                logger.info(f"  {w['address']} (seen {last_seen_str})")
        else:
            logger.info("No wallets found in the network")
    
    elif args.command == 'backup':
        try:
            with open(args.file, 'w') as f:
                f.write(f"Wallet Address: {wallet.address}\n")
                f.write(f"Private Key: {base64.b64encode(wallet.private_key).decode('utf-8')}\n")
                f.write("WARNING: Keep this file secure and never share it with anyone!\n")
            logger.info(f"✅ Wallet backed up to {args.file}")
            logger.info("🔒 IMPORTANT: Store this file securely! Anyone with your private key can access your funds.")
        except Exception as e:
            logger.error(f"❌ Backup failed: {e}")
    
    elif args.command == 'fund':
        # For development only - create test funds
        try:
            wallet.balance += args.amount
            wallet._save_wallet()
            logger.info(f"✅ Added {args.amount} test funds")
            logger.info(f"💰 New balance: {wallet.balance}")
            logger.warning("⚠️ This is for development/testing only!")
        except Exception as e:
            logger.error(f"❌ Failed to add test funds: {e}")
    
    elif args.command == 'history':
        # Sync wallet first
        await wallet_dht.sync_wallet()
        
        # Get transaction history
        if wallet.transactions:
            logger.info(f"Transaction history ({len(wallet.transactions)} transactions):")
            for i, tx in enumerate(sorted(wallet.transactions, key=lambda x: x['timestamp'], reverse=True)):
                timestamp = datetime.fromtimestamp(tx['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                if tx['sender'] == wallet.address:
                    logger.info(f"  {i+1}. SENT {tx['amount']} to {tx['recipient']} ({timestamp})")
                else:
                    logger.info(f"  {i+1}. RECEIVED {tx['amount']} from {tx['sender']} ({timestamp})")
                if tx.get('memo'):
                    logger.info(f"     Memo: {tx['memo']}")
        else:
            logger.info("No transactions found")
    
    # Allow some time for operations to complete
    await asyncio.sleep(2)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
