import os
import click

import brownie
import pandas

from utils.contract_utils import init_contract
from utils.curve_utils.registry import get_eth_pools
from utils.eth_blocks_utils import get_block_for_datestring
from utils.network_utils import connect_if_not_connect


@click.command()
@click.option("--date-start", "date_start", type=str)
@click.option("--date-end", "date_end", type=str)
@click.option("--filename", "filename", type=str)
def main(date_start: int, date_end: int, filename: str):

    block_start = get_block_for_datestring(date_start)
    block_end = get_block_for_datestring(date_end)

    connect_if_not_connect()
    eth_pools = get_eth_pools()

    swap_revenue_data = pandas.DataFrame()
    if os.path.exists(filename):
        swap_revenue_data = pandas.read_parquet(filename)

    for block_number in range(block_start, block_end):

        timestamp = brownie.web3.eth.getBlock(block_number)['timestamp']

        with brownie.multicall(block_identifier=block_number):

            for pool_name, pool_details in eth_pools.items():

                if pool_data_exists(
                        pool_name,
                        block_number,
                        swap_revenue_data
                ):
                    continue

                pool_data = {}

                pool_addr = pool_details[0]
                gauge_addr = pool_details[1]
                lp_token_addr = pool_details[2]

                lp_token_contract = init_contract(lp_token_addr)
                pool_contract = init_contract(pool_addr)

                lp_data = get_liquidity_pool_data(
                    pool_contract=pool_contract,
                    lp_token_contract=lp_token_contract
                )

                # pool data
                pool_data['timestamp'] = timestamp
                pool_data['block_number'] = block_number
                pool_data['pool_name'] = pool_name
                pool_data['pool_addr'] = pool_addr
                pool_data['lp_token_addr'] = lp_token_addr
                pool_data['gauge_addr'] = gauge_addr
                pool_data['lp_token_virtual_price'] = lp_data['virtual_price']
                pool_data['total_supply_lp_token'] = lp_data['total_supply_lp_token']

                pool_data = pandas.DataFrame(
                    data=pool_data,
                    index=[pandas.to_datetime(pool_data['timestamp'], unit='s')]
                )

                # join df
                swap_revenue_data = pandas.concat([swap_revenue_data, pool_data])

    swap_revenue_data.to_parquet(filename)


def pool_data_exists(
        pool_name: str,
        block_number: int,
        data: pandas.DataFrame
):
    return (
            (data['block_number'] == block_number) &
            (data['pool_name'] == pool_name)
    ).any()


def get_liquidity_pool_data(
    pool_contract: brownie.Contract,
    lp_token_contract: brownie.Contract,
):
    lp_token_virtual_price = pool_contract.get_virtual_price() * 1e-18
    total_supply_lp_token = lp_token_contract.totalSupply() * 1e-18

    return {
        'virtual_price': lp_token_virtual_price,
        'total_supply_lp_token': total_supply_lp_token
    }


if __name__ == "__main__":
    main()
