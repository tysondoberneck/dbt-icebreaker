"""
Snowflake Helper for Icebreaker Adapter

Provides standalone Snowflake connection helpers for source caching
and fallback execution. Reads credentials from dbt profiles.yml
and supports RSA key-pair authentication.
"""

import os
import yaml
from typing import Any, Dict, Optional


def find_icebreaker_profile() -> Optional[Dict[str, Any]]:
    """
    Find and return the icebreaker profile configuration from profiles.yml.
    
    Searches in standard dbt profile locations:
    1. DBT_PROFILES_DIR environment variable
    2. Current directory
    3. ~/.dbt/profiles.yml
    
    Returns the target output dict (e.g., account, database, schema, etc.)
    or None if not found.
    """
    profiles_paths = []
    
    # Check DBT_PROFILES_DIR env var first
    env_dir = os.environ.get("DBT_PROFILES_DIR")
    if env_dir:
        profiles_paths.append(os.path.join(env_dir, "profiles.yml"))
    
    # Current directory
    profiles_paths.append(os.path.join(os.getcwd(), "profiles.yml"))
    
    # Default ~/.dbt/
    profiles_paths.append(os.path.expanduser("~/.dbt/profiles.yml"))
    
    for path in profiles_paths:
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    profiles = yaml.safe_load(f)
                
                if not profiles:
                    continue
                
                # Look for icebreaker_dev or any profile with type: icebreaker
                for profile_name, profile_config in profiles.items():
                    if not isinstance(profile_config, dict):
                        continue
                    
                    outputs = profile_config.get("outputs", {})
                    target_name = profile_config.get("target", "dev")
                    
                    # Check each output for icebreaker type
                    for output_name, output_config in outputs.items():
                        if not isinstance(output_config, dict):
                            continue
                        if output_config.get("type") == "icebreaker":
                            return output_config
                    
                    # Also check the default target
                    if target_name in outputs:
                        target_config = outputs[target_name]
                        if isinstance(target_config, dict) and target_config.get("type") == "icebreaker":
                            return target_config
                
            except Exception as e:
                print(f"⚠️ Could not read profiles from {path}: {e}")
                continue
    
    return None


def get_snowflake_connection() -> Optional[Any]:
    """
    Create a Snowflake connection using credentials from the icebreaker profile.
    
    Supports:
    - RSA key-pair authentication (private_key_path)
    - Password authentication
    - External browser / SSO authenticator
    
    Returns a snowflake.connector connection object, or None if unavailable.
    """
    try:
        import snowflake.connector
    except ImportError:
        print("⚠️ snowflake-connector-python not installed. Run: pip install snowflake-connector-python")
        return None
    
    profile = find_icebreaker_profile()
    if not profile:
        print("⚠️ No icebreaker profile found in profiles.yml")
        return None
    
    account = profile.get("account")
    if not account:
        print("⚠️ No Snowflake account configured in icebreaker profile")
        return None
    
    connect_kwargs: Dict[str, Any] = {
        "account": account,
    }
    
    # User
    user = profile.get("user")
    if user:
        connect_kwargs["user"] = user
    
    # Database and schema
    database = profile.get("database")
    if database and database not in ("memory", "main"):
        connect_kwargs["database"] = database
    
    schema = profile.get("schema")
    if schema and schema not in ("main",):
        connect_kwargs["schema"] = schema
    
    # Warehouse and role
    warehouse = profile.get("warehouse")
    if warehouse:
        connect_kwargs["warehouse"] = warehouse
    
    role = profile.get("role")
    if role:
        connect_kwargs["role"] = role
    
    # === Authentication ===
    private_key_path = profile.get("private_key_path")
    password = profile.get("password")
    authenticator = profile.get("authenticator")
    
    if private_key_path:
        # RSA key-pair authentication
        try:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
            
            key_path = os.path.expanduser(private_key_path)
            with open(key_path, "rb") as key_file:
                passphrase = profile.get("private_key_passphrase")
                p_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=passphrase.encode() if passphrase else None,
                    backend=default_backend(),
                )
            
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            connect_kwargs["private_key"] = pkb
        except Exception as e:
            print(f"⚠️ Could not load private key from {private_key_path}: {e}")
            return None
    elif password:
        connect_kwargs["password"] = password
    elif authenticator:
        connect_kwargs["authenticator"] = authenticator
    else:
        print("⚠️ No Snowflake auth method found (need private_key_path, password, or authenticator)")
        return None
    
    try:
        conn = snowflake.connector.connect(**connect_kwargs)
        return conn
    except Exception as e:
        print(f"⚠️ Snowflake connection failed: {e}")
        return None
