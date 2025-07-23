"""
Simple configuration for Pulsar Financial Message Producer
Local development setup with minimal complexity
"""

# =============================================================================
# Producer Configuration
# =============================================================================

PRODUCER_CONFIG = {
    'compression_type': 'LZ4',  # Options: NONE, LZ4 (ZLIB and ZSTD may not be available in all versions)
    'batching_enabled': True,
    'batch_max_messages': 100,
    'batch_max_delay_ms': 100,
    'send_timeout_ms': 30000,
    'max_pending_messages': 1000
}

# =============================================================================
# Environment Configurations
# =============================================================================

DEV_CONFIG = {
    'service_url': 'pulsar://localhost:6650',
    'topic': 'financial-messages',
    'auth': {},  # No authentication needed for local Pulsar standalone
    'log_level': 'INFO'
}

PROD_CONFIG = {
    'service_url': 'pulsar://44.247.85.233:6650',
    'topic': 'financial-messages',
    'auth': {},  # Or JWT/OAuth2 if configured
    'log_level': 'WARNING'
}

# =============================================================================
# Default Message Configuration
# =============================================================================

DEFAULT_INSTRUMENT_CONFIG = {
    'currency': 'USD',
    'company': 'Financial Corp',
    'account_side': 'Asset',
    'discount_curve': 'TreasuryYield',
    'portfolio_prefix': 'CORPORATE_BONDS',
    'instrument_type': 'Bond',
    'day_count': '30/360'
}

DEFAULT_RISK_CONFIG = {
    'scenario_identifier': 'Base',
    'model_name': 'Standard Risk Model',
    'model_output': 'Risk Assessment',
    'default_lgd': 0.45,  # 45% Loss Given Default
    'default_pd': 0.012,  # 1.2% Probability of Default
    'risk_weight': 0.75   # 75% risk weight
}

# =============================================================================
# Usage Example
# =============================================================================

"""
Example usage in your producer/consumer scripts:

from config import DEV_CONFIG, PRODUCER_CONFIG

# Use the simplified config
producer = PulsarFinancialMessageProducer(
    service_url=DEV_CONFIG['service_url'],
    topic=DEV_CONFIG['topic'],
    auth_params=DEV_CONFIG['auth']
)
""" 