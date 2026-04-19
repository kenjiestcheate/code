"""
data_manager.py
Gestionnaire centralisé pour tous les fichiers JSON.
Handles CRUD operations, validation, and persistence.
~2500 lignes de code
"""

import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import threading
import logging
from copy import deepcopy
import hashlib

# ============================================================================
# IMPORTS LOCAUX
# ============================================================================

try:
    from config import Config, LOGGER
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config import Config, LOGGER

# ============================================================================
# CONSTANTES
# ============================================================================

BACKUP_DIR = Config.DATA_DIR / "backups"
BACKUP_DIR.mkdir(exist_ok=True)

# ============================================================================
# CLASSE DE GESTION DES DONNÉES
# ============================================================================

class DataManager:
    """Gestionnaire centralisé pour toutes les données JSON."""
    
    # Singleton instance
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Implémente le pattern Singleton thread-safe."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialise le gestionnaire de données."""
        if not hasattr(self, '_initialized'):
            self._data = {}
            self._file_timestamps = {}
            self._change_callbacks = {}
            self._lock = threading.RLock()
            self._load_all_data()
            self._initialized = True
            LOGGER.info("DataManager initialisé ✓")
    
    # ========================================================================
    # CHARGEMENT ET SAUVEGARDE
    # ========================================================================
    
    def _load_all_data(self) -> bool:
        """Charge toutes les données JSON au démarrage."""
        try:
            self._data = {
                'products': self._load_json(Config.PRODUCTS_FILE, self._default_products()),
                'users': self._load_json(Config.USERS_FILE, self._default_users()),
                'cards': self._load_json(Config.CARDS_FILE, self._default_cards()),
                'transactions': self._load_json(Config.TRANSACTIONS_FILE, self._default_transactions()),
            }
            LOGGER.info("Toutes les données chargées ✓")
            return True
        except Exception as e:
            LOGGER.error(f"Erreur chargement données: {e}")
            return False
    
    def _load_json(self, filepath: Path, default: Dict = None) -> Dict:
        """Charge un fichier JSON avec gestion d'erreurs."""
        try:
            if filepath.exists() and filepath.stat().st_size > 0:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self._file_timestamps[str(filepath)] = os.path.getmtime(filepath)
                    return data
            else:
                LOGGER.warning(f"Fichier non trouvé: {filepath}, création par défaut")
                if default:
                    self._save_json(filepath, default)
                return default or {}
        except json.JSONDecodeError as e:
            LOGGER.error(f"Erreur JSON {filepath}: {e}")
            return default or {}
        except Exception as e:
            LOGGER.error(f"Erreur lecture {filepath}: {e}")
            return default or {}
    
    def _save_json(self, filepath: Path, data: Dict) -> bool:
        """Sauvegarde les données dans un fichier JSON."""
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # Créer une sauvegarde avant remplacement
            if filepath.exists():
                self._create_backup(filepath)
            
            # Écrire les nouvelles données
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            self._file_timestamps[str(filepath)] = os.path.getmtime(filepath)
            LOGGER.debug(f"Données sauvegardées: {filepath}")
            return True
        except Exception as e:
            LOGGER.error(f"Erreur sauvegarde {filepath}: {e}")
            return False
    
    def _create_backup(self, filepath: Path) -> None:
        """Crée une sauvegarde d'un fichier avant modification."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{filepath.stem}_{timestamp}.backup"
            backup_path = BACKUP_DIR / backup_name
            
            with open(filepath, 'r', encoding='utf-8') as f:
                data = f.read()
            
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.write(data)
            
            LOGGER.debug(f"Sauvegarde créée: {backup_path}")
        except Exception as e:
            LOGGER.warning(f"Impossible créer sauvegarde: {e}")
    
    # ========================================================================
    # DONNÉES PAR DÉFAUT
    # ========================================================================
    
    @staticmethod
    def _default_products() -> Dict:
        """Retourne la structure par défaut des produits."""
        return {
            "products": [],
            "categories": Config.DEFAULT_CATEGORIES,
            "metadata": {
                "version": "1.0",
                "total_products": 0,
                "last_updated": datetime.now().isoformat() + "Z",
                "currency": "EUR"
            }
        }
    
    @staticmethod
    def _default_users() -> Dict:
        """Retourne la structure par défaut des utilisateurs."""
        return {
            "users": [],
            "roles": ["admin", "cashier", "customer", "manager"],
            "metadata": {
                "version": "1.0",
                "total_users": 0,
                "last_updated": datetime.now().isoformat() + "Z"
            }
        }
    
    @staticmethod
    def _default_cards() -> Dict:
        """Retourne la structure par défaut des cartes."""
        return {
            "cards": [],
            "metadata": {
                "version": "1.0",
                "total_cards": 0,
                "total_balance": 0.00,
                "last_updated": datetime.now().isoformat() + "Z"
            }
        }
    
    @staticmethod
    def _default_transactions() -> Dict:
        """Retourne la structure par défaut des transactions."""
        return {
            "transactions": [],
            "metadata": {
                "version": "1.0",
                "total_transactions": 0,
                "total_sales_amount": 0.00,
                "total_refunds_amount": 0.00,
                "total_reloads_amount": 0.00,
                "net_amount": 0.00,
                "last_updated": datetime.now().isoformat() + "Z",
                "period": {
                    "start": datetime.now().isoformat() + "Z",
                    "end": datetime.now().isoformat() + "Z"
                }
            }
        }
    
    # ========================================================================
    # PRODUITS - CRUD
    # ========================================================================
    
    def add_product(self, product: Dict) -> Tuple[bool, str]:
        """Ajoute un nouveau produit."""
        try:
            with self._lock:
                # Validation
                if not product.get('name'):
                    return False, "Nom produit requis"
                if product.get('price', 0) < 0:
                    return False, "Prix invalide"
                
                # Générer ID unique
                if not product.get('id'):
                    product['id'] = f"PROD_{len(self._data['products']['products']) + 1:06d}"
                
                # Ajouter métadonnées
                product['created_at'] = datetime.now().isoformat() + "Z"
                product['updated_at'] = datetime.now().isoformat() + "Z"
                product['active'] = product.get('active', True)
                product['stock'] = product.get('stock', 0)
                product['min_stock'] = product.get('min_stock', 0)
                
                # Ajouter
                self._data['products']['products'].append(product)
                self._data['products']['metadata']['total_products'] += 1
                self._data['products']['metadata']['last_updated'] = datetime.now().isoformat() + "Z"
                
                self._save_json(Config.PRODUCTS_FILE, self._data['products'])
                self._trigger_callback('products', 'add')
                
                LOGGER.info(f"Produit ajouté: {product['id']}")
                return True, f"Produit ajouté: {product['id']}"
        
        except Exception as e:
            LOGGER.error(f"Erreur ajout produit: {e}")
            return False, str(e)
    
    def get_product(self, product_id: str) -> Optional[Dict]:
        """Récupère un produit par ID."""
        try:
            with self._lock:
                for product in self._data['products']['products']:
                    if product['id'] == product_id:
                        return deepcopy(product)
                return None
        except Exception as e:
            LOGGER.error(f"Erreur récupération produit: {e}")
            return None
    
    def get_all_products(self, active_only: bool = False) -> List[Dict]:
        """Récupère tous les produits."""
        try:
            with self._lock:
                products = self._data['products']['products']
                if active_only:
                    products = [p for p in products if p.get('active', True)]
                return deepcopy(products)
        except Exception as e:
            LOGGER.error(f"Erreur récupération produits: {e}")
            return []
    
    def update_product(self, product_id: str, updates: Dict) -> Tuple[bool, str]:
        """Modifie un produit existant."""
        try:
            with self._lock:
                for i, product in enumerate(self._data['products']['products']):
                    if product['id'] == product_id:
                        # Validation prix
                        if 'price' in updates and updates['price'] < 0:
                            return False, "Prix invalide"
                        
                        # Validation stock
                        if 'stock' in updates and updates['stock'] < 0:
                            return False, "Stock invalide"
                        
                        # Mise à jour
                        self._data['products']['products'][i].update(updates)
                        self._data['products']['products'][i]['updated_at'] = datetime.now().isoformat() + "Z"
                        self._data['products']['metadata']['last_updated'] = datetime.now().isoformat() + "Z"
                        
                        self._save_json(Config.PRODUCTS_FILE, self._data['products'])
                        self._trigger_callback('products', 'update')
                        
                        LOGGER.info(f"Produit modifié: {product_id}")
                        return True, "Produit modifié"
                
                return False, "Produit non trouvé"
        
        except Exception as e:
            LOGGER.error(f"Erreur modification produit: {e}")
            return False, str(e)
    
    def delete_product(self, product_id: str) -> Tuple[bool, str]:
        """Supprime un produit."""
        try:
            with self._lock:
                original_count = len(self._data['products']['products'])
                self._data['products']['products'] = [
                    p for p in self._data['products']['products']
                    if p['id'] != product_id
                ]
                
                if len(self._data['products']['products']) < original_count:
                    self._data['products']['metadata']['total_products'] -= 1
                    self._data['products']['metadata']['last_updated'] = datetime.now().isoformat() + "Z"
                    self._save_json(Config.PRODUCTS_FILE, self._data['products'])
                    self._trigger_callback('products', 'delete')
                    LOGGER.info(f"Produit supprimé: {product_id}")
                    return True, "Produit supprimé"
                else:
                    return False, "Produit non trouvé"
        
        except Exception as e:
            LOGGER.error(f"Erreur suppression produit: {e}")
            return False, str(e)
    
    def search_products(self, query: str) -> List[Dict]:
        """Recherche les produits par nom ou SKU."""
        try:
            with self._lock:
                query = query.lower()
                results = [
                    p for p in self._data['products']['products']
                    if query in p.get('name', '').lower() or
                       query in p.get('sku', '').lower()
                ]
                return deepcopy(results)
        except Exception as e:
            LOGGER.error(f"Erreur recherche produits: {e}")
            return []
    
    def get_products_by_category(self, category: str) -> List[Dict]:
        """Récupère les produits d'une catégorie."""
        try:
            with self._lock:
                products = [
                    p for p in self._data['products']['products']
                    if p.get('category') == category
                ]
                return deepcopy(products)
        except Exception as e:
            LOGGER.error(f"Erreur filtre catégorie: {e}")
            return []
    
    # ========================================================================
    # UTILISATEURS - CRUD
    # ========================================================================
    
    def add_user(self, user: Dict) -> Tuple[bool, str]:
        """Ajoute un nouvel utilisateur."""
        try:
            with self._lock:
                # Validation
                if not user.get('name'):
                    return False, "Nom utilisateur requis"
                if len(user.get('name', '')) < Config.USER_NAME_MIN_LENGTH:
                    return False, f"Nom trop court (min {Config.USER_NAME_MIN_LENGTH})"
                
                # Générer ID unique
                if not user.get('id'):
                    user['id'] = f"USER_{len(self._data['users']['users']) + 1:06d}"
                
                # Ajouter métadonnées
                user['created_at'] = datetime.now().isoformat() + "Z"
                user['updated_at'] = datetime.now().isoformat() + "Z"
                user['active'] = user.get('active', True)
                user['balance'] = user.get('balance', 0.0)
                user['loyalty_points'] = user.get('loyalty_points', 0)
                
                self._data['users']['users'].append(user)
                self._data['users']['metadata']['total_users'] += 1
                self._data['users']['metadata']['last_updated'] = datetime.now().isoformat() + "Z"
                
                self._save_json(Config.USERS_FILE, self._data['users'])
                self._trigger_callback('users', 'add')
                
                LOGGER.info(f"Utilisateur ajouté: {user['id']}")
                return True, f"Utilisateur ajouté: {user['id']}"
        
        except Exception as e:
            LOGGER.error(f"Erreur ajout utilisateur: {e}")
            return False, str(e)
    
    def get_user(self, user_id: str) -> Optional[Dict]:
        """Récupère un utilisateur par ID."""
        try:
            with self._lock:
                for user in self._data['users']['users']:
                    if user['id'] == user_id:
                        return deepcopy(user)
                return None
        except Exception as e:
            LOGGER.error(f"Erreur récupération utilisateur: {e}")
            return None
    
    def get_all_users(self, active_only: bool = False) -> List[Dict]:
        """Récupère tous les utilisateurs."""
        try:
            with self._lock:
                users = self._data['users']['users']
                if active_only:
                    users = [u for u in users if u.get('active', True)]
                return deepcopy(users)
        except Exception as e:
            LOGGER.error(f"Erreur récupération utilisateurs: {e}")
            return []
    
    def update_user(self, user_id: str, updates: Dict) -> Tuple[bool, str]:
        """Modifie un utilisateur."""
        try:
            with self._lock:
                for i, user in enumerate(self._data['users']['users']):
                    if user['id'] == user_id:
                        self._data['users']['users'][i].update(updates)
                        self._data['users']['users'][i]['updated_at'] = datetime.now().isoformat() + "Z"
                        self._data['users']['metadata']['last_updated'] = datetime.now().isoformat() + "Z"
                        
                        self._save_json(Config.USERS_FILE, self._data['users'])
                        self._trigger_callback('users', 'update')
                        
                        LOGGER.info(f"Utilisateur modifié: {user_id}")
                        return True, "Utilisateur modifié"
                
                return False, "Utilisateur non trouvé"
        
        except Exception as e:
            LOGGER.error(f"Erreur modification utilisateur: {e}")
            return False, str(e)
    
    def delete_user(self, user_id: str) -> Tuple[bool, str]:
        """Supprime un utilisateur."""
        try:
            with self._lock:
                original_count = len(self._data['users']['users'])
                self._data['users']['users'] = [
                    u for u in self._data['users']['users']
                    if u['id'] != user_id
                ]
                
                if len(self._data['users']['users']) < original_count:
                    self._data['users']['metadata']['total_users'] -= 1
                    self._data['users']['metadata']['last_updated'] = datetime.now().isoformat() + "Z"
                    self._save_json(Config.USERS_FILE, self._data['users'])
                    self._trigger_callback('users', 'delete')
                    LOGGER.info(f"Utilisateur supprimé: {user_id}")
                    return True, "Utilisateur supprimé"
                else:
                    return False, "Utilisateur non trouvé"
        
        except Exception as e:
            LOGGER.error(f"Erreur suppression utilisateur: {e}")
            return False, str(e)
    
    # ========================================================================
    # CARTES RFID - CRUD
    # ========================================================================
    
    def add_card(self, card: Dict) -> Tuple[bool, str]:
        """Ajoute une nouvelle carte RFID."""
        try:
            with self._lock:
                # Validation
                if not card.get('uid'):
                    return False, "UID carte requis"
                
                # Vérifier unicité UID
                for existing_card in self._data['cards']['cards']:
                    if existing_card['uid'] == card['uid']:
                        return False, f"UID déjà existant: {card['uid']}"
                
                # Générer ID unique
                if not card.get('id'):
                    card['id'] = f"CARD_{len(self._data['cards']['cards']) + 1:06d}"
                
                # Ajouter métadonnées
                card['created_at'] = datetime.now().isoformat() + "Z"
                card['updated_at'] = datetime.now().isoformat() + "Z"
                card['is_active'] = card.get('is_active', True)
                card['is_blocked'] = card.get('is_blocked', False)
                card['balance'] = card.get('balance', Config.DEFAULT_CARD_BALANCE)
                card['transaction_count'] = card.get('transaction_count', 0)
                card['total_spent'] = card.get('total_spent', 0.0)
                
                self._data['cards']['cards'].append(card)
                self._data['cards']['metadata']['total_cards'] += 1
                self._data['cards']['metadata']['total_balance'] = self._calculate_total_balance()
                self._data['cards']['metadata']['last_updated'] = datetime.now().isoformat() + "Z"
                
                self._save_json(Config.CARDS_FILE, self._data['cards'])
                self._trigger_callback('cards', 'add')
                
                LOGGER.info(f"Carte ajoutée: {card['id']} (UID: {card['uid']})")
                return True, f"Carte ajoutée: {card['id']}"
        
        except Exception as e:
            LOGGER.error(f"Erreur ajout carte: {e}")
            return False, str(e)
    
    def get_card_by_uid(self, uid: str) -> Optional[Dict]:
        """Récupère une carte par son UID."""
        try:
            with self._lock:
                for card in self._data['cards']['cards']:
                    if card['uid'].upper() == uid.upper():
                        return deepcopy(card)
                return None
        except Exception as e:
            LOGGER.error(f"Erreur récupération carte: {e}")
            return None
    
    def get_card(self, card_id: str) -> Optional[Dict]:
        """Récupère une carte par ID."""
        try:
            with self._lock:
                for card in self._data['cards']['cards']:
                    if card['id'] == card_id:
                        return deepcopy(card)
                return None
        except Exception as e:
            LOGGER.error(f"Erreur récupération carte: {e}")
            return None
    
    def get_all_cards(self, active_only: bool = False) -> List[Dict]:
        """Récupère toutes les cartes."""
        try:
            with self._lock:
                cards = self._data['cards']['cards']
                if active_only:
                    cards = [c for c in cards if c.get('is_active', True) and not c.get('is_blocked', False)]
                return deepcopy(cards)
        except Exception as e:
            LOGGER.error(f"Erreur récupération cartes: {e}")
            return []
    
    def update_card(self, card_id: str, updates: Dict) -> Tuple[bool, str]:
        """Modifie une carte."""
        try:
            with self._lock:
                for i, card in enumerate(self._data['cards']['cards']):
                    if card['id'] == card_id:
                        self._data['cards']['cards'][i].update(updates)
                        self._data['cards']['cards'][i]['updated_at'] = datetime.now().isoformat() + "Z"
                        self._data['cards']['metadata']['total_balance'] = self._calculate_total_balance()
                        self._data['cards']['metadata']['last_updated'] = datetime.now().isoformat() + "Z"
                        
                        self._save_json(Config.CARDS_FILE, self._data['cards'])
                        self._trigger_callback('cards', 'update')
                        
                        LOGGER.info(f"Carte modifiée: {card_id}")
                        return True, "Carte modifiée"
                
                return False, "Carte non trouvée"
        
        except Exception as e:
            LOGGER.error(f"Erreur modification carte: {e}")
            return False, str(e)
    
    def delete_card(self, card_id: str) -> Tuple[bool, str]:
        """Supprime une carte."""
        try:
            with self._lock:
                original_count = len(self._data['cards']['cards'])
                self._data['cards']['cards'] = [
                    c for c in self._data['cards']['cards']
                    if c['id'] != card_id
                ]
                
                if len(self._data['cards']['cards']) < original_count:
                    self._data['cards']['metadata']['total_cards'] -= 1
                    self._data['cards']['metadata']['total_balance'] = self._calculate_total_balance()
                    self._data['cards']['metadata']['last_updated'] = datetime.now().isoformat() + "Z"
                    self._save_json(Config.CARDS_FILE, self._data['cards'])
                    self._trigger_callback('cards', 'delete')
                    LOGGER.info(f"Carte supprimée: {card_id}")
                    return True, "Carte supprimée"
                else:
                    return False, "Carte non trouvée"
        
        except Exception as e:
            LOGGER.error(f"Erreur suppression carte: {e}")
            return False, str(e)
    
    # ========================================================================
    # TRANSACTIONS
    # ========================================================================
    
    def add_transaction(self, transaction: Dict) -> Tuple[bool, str]:
        """Ajoute une nouvelle transaction."""
        try:
            with self._lock:
                # Validation
                if transaction.get('amount', 0) == 0:
                    return False, "Montant requis"
                
                # Générer ID unique
                if not transaction.get('id'):
                    transaction['id'] = f"TX_{len(self._data['transactions']['transactions']) + 1:06d}"
                
                # Ajouter métadonnées
                transaction['created_at'] = datetime.now().isoformat() + "Z"
                transaction['completed_at'] = transaction.get('completed_at', datetime.now().isoformat() + "Z")
                transaction['status'] = transaction.get('status', 'pending')
                
                self._data['transactions']['transactions'].append(transaction)
                self._data['transactions']['metadata']['total_transactions'] += 1
                self._data['transactions']['metadata']['last_updated'] = datetime.now().isoformat() + "Z"
                
                self._save_json(Config.TRANSACTIONS_FILE, self._data['transactions'])
                self._trigger_callback('transactions', 'add')
                
                LOGGER.info(f"Transaction ajoutée: {transaction['id']}")
                return True, f"Transaction ajoutée: {transaction['id']}"
        
        except Exception as e:
            LOGGER.error(f"Erreur ajout transaction: {e}")
            return False, str(e)
    
    def get_transaction(self, transaction_id: str) -> Optional[Dict]:
        """Récupère une transaction par ID."""
        try:
            with self._lock:
                for tx in self._data['transactions']['transactions']:
                    if tx['id'] == transaction_id:
                        return deepcopy(tx)
                return None
        except Exception as e:
            LOGGER.error(f"Erreur récupération transaction: {e}")
            return None
    
    def get_all_transactions(self) -> List[Dict]:
        """Récupère toutes les transactions."""
        try:
            with self._lock:
                return deepcopy(self._data['transactions']['transactions'])
        except Exception as e:
            LOGGER.error(f"Erreur récupération transactions: {e}")
            return []
    
    def get_transactions_for_card(self, card_uid: str, limit: int = 50) -> List[Dict]:
        """Récupère l'historique d'une carte."""
        try:
            with self._lock:
                transactions = [
                    tx for tx in self._data['transactions']['transactions']
                    if tx.get('card_uid', '').upper() == card_uid.upper()
                ]
                return deepcopy(sorted(transactions, 
                                      key=lambda x: x.get('created_at', ''), 
                                      reverse=True)[:limit])
        except Exception as e:
            LOGGER.error(f"Erreur récupération historique: {e}")
            return []
    
    # ========================================================================
    # UTILITAIRES
    # ========================================================================
    
    def _calculate_total_balance(self) -> float:
        """Calcule le solde total de toutes les cartes."""
        try:
            total = sum(card.get('balance', 0.0) for card in self._data['cards']['cards'])
            return round(total, Config.DECIMAL_PLACES)
        except:
            return 0.0
    
    def _trigger_callback(self, data_type: str, action: str) -> None:
        """Déclenche les callbacks enregistrés."""
        callback_key = f"{data_type}:{action}"
        if callback_key in self._change_callbacks:
            try:
                self._change_callbacks[callback_key]()
            except Exception as e:
                LOGGER.error(f"Erreur callback: {e}")
    
    def register_callback(self, data_type: str, action: str, callback) -> None:
        """Enregistre un callback pour les changements."""
        callback_key = f"{data_type}:{action}"
        self._change_callbacks[callback_key] = callback
    
    def get_statistics(self) -> Dict:
        """Retourne les statistiques globales."""
        try:
            with self._lock:
                products = self._data['products']['products']
                users = self._data['users']['users']
                cards = self._data['cards']['cards']
                transactions = self._data['transactions']['transactions']
                
                return {
                    'products': {
                        'total': len(products),
                        'active': len([p for p in products if p.get('active', True)]),
                        'low_stock': len([p for p in products if p.get('stock', 0) <= Config.LOW_STOCK_THRESHOLD])
                    },
                    'users': {
                        'total': len(users),
                        'active': len([u for u in users if u.get('active', True)])
                    },
                    'cards': {
                        'total': len(cards),
                        'active': len([c for c in cards if c.get('is_active', True)]),
                        'total_balance': self._calculate_total_balance()
                    },
                    'transactions': {
                        'total': len(transactions),
                        'completed': len([t for t in transactions if t.get('status') == 'completed']),
                        'failed': len([t for t in transactions if t.get('status') == 'failed'])
                    }
                }
        except Exception as e:
            LOGGER.error(f"Erreur statistiques: {e}")
            return {}

# ============================================================================
# INITIALISATION
# ============================================================================

if __name__ == "__main__":
    dm = DataManager()
    stats = dm.get_statistics()
    print("✓ DataManager prêt")
    print(f"Statistiques: {stats}")