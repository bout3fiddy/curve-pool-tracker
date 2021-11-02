from utils.contract_utils import init_contract
from utils.network_utils import connect_if_not_connect

ETHEREUM_REGISTRY = "0x90E00ACe148ca3b23Ac1bC8C240C2a7Dd9c2d7f5"


def get_pools(registry: str):
    connect_if_not_connect()
    registry_contract = init_contract(registry)

    pool_count = registry_contract.pool_count()
    data = {}
    for pool_id in range(pool_count):

        pool_addr = registry_contract.pool_list(pool_id)
        gauge_addr = registry_contract.get_gauges(pool_addr)
        lp_token_addr = registry_contract.get_lp_token(pool_addr)
        pool_token_contract = init_contract(lp_token_addr)
        pool_token_name = pool_token_contract.name()

        data[pool_token_name] = [pool_addr, gauge_addr, lp_token_addr]

    return data


def get_eth_pools() -> dict:

    return get_pools(ETHEREUM_REGISTRY)
