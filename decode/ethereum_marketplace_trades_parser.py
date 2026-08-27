"""Pure decoder for V3/V4 marketplace Traded logs.

No eth_abi at runtime: the parser walks the ABI layout word by word.
Trade = (signer, bytes signature, Checks checks, Asset[] sent,
Asset[] received); Asset = (assetType, contractAddress, value,
beneficiary, bytes extra). Because `extra` is dynamic, array elements
carry per-element offsets — unlike Seaport's static items. signature,
checks and extra are skipped entirely. The manual parse is validated
against eth_abi in tests.

sent = what the signer gives, received = what the signer gets, and the
indexed caller is always the executing counterparty — so buyer/seller
are never NULL (unlike Seaport's zero-address recipient case).
"""

from decode.ethereum_marketplace_trades_query import TRADED_TOPIC

_ASSET_NFT = {3, 4}  # ERC-721, collection item (primary mint)
_ASSET_PAYMENT = {1, 2}  # ERC-20, USD-pegged MANA


def _word(data_hex: str, index: int) -> str:
    word = data_hex[index * 64 : (index + 1) * 64]
    if len(word) < 64:
        raise ValueError(f"data truncated at word {index}")
    return word


def _uint(data_hex: str, index: int) -> int:
    return int(_word(data_hex, index), 16)


def _address(word: str) -> str:
    return ("0x" + word[-40:]).lower()


def _read_assets(data_hex: str, array_word: int) -> list[dict]:
    count = _uint(data_hex, array_word)
    # dynamic elements: per-element offsets, relative to the word after
    # the array length
    base = array_word + 1
    assets = []
    for i in range(count):
        elem = base + _uint(data_hex, base + i) // 32
        assets.append(
            {
                "asset_type": _uint(data_hex, elem),
                "contract": _address(_word(data_hex, elem + 1)),
                "value": _uint(data_hex, elem + 2),
                "beneficiary": _address(_word(data_hex, elem + 3)),
            }
        )
    return assets


def parse_traded(topics: list[str], data: str) -> dict:
    if not topics or topics[0] != TRADED_TOPIC:
        raise ValueError(f"not a Traded topic: {topics[:1]}")
    caller = _address(topics[1])
    trade_id = topics[2]

    data_hex = data.removeprefix("0x")
    trade = _uint(data_hex, 0) // 32  # offset to the Trade tuple
    signer = _address(_word(data_hex, trade))
    # tuple field offsets are relative to the tuple base; fields:
    # 0 signer, 1 signature, 2 checks, 3 sent, 4 received
    sent = _read_assets(data_hex, trade + _uint(data_hex, trade + 3) // 32)
    received = _read_assets(data_hex, trade + _uint(data_hex, trade + 4) // 32)

    nft_contracts: list[str] = []
    nft_token_ids: list[str] = []
    nft_asset_types: list[int] = []
    nft_beneficiaries: list[str] = []
    pay_currencies: list[str] = []
    pay_amounts: list[int] = []
    pay_asset_types: list[int] = []
    pay_beneficiaries: list[str] = []
    nft_in_sent = nft_in_received = False

    for side, assets in (("sent", sent), ("received", received)):
        for asset in assets:
            if asset["asset_type"] in _ASSET_NFT:
                if side == "sent":
                    nft_in_sent = True
                else:
                    nft_in_received = True
                nft_contracts.append(asset["contract"])
                nft_token_ids.append(str(asset["value"]))
                nft_asset_types.append(asset["asset_type"])
                nft_beneficiaries.append(asset["beneficiary"])
            elif asset["asset_type"] in _ASSET_PAYMENT:
                pay_currencies.append(asset["contract"])
                pay_amounts.append(asset["value"])
                pay_asset_types.append(asset["asset_type"])
                pay_beneficiaries.append(asset["beneficiary"])
            else:
                raise ValueError(
                    f"unexpected assetType {asset['asset_type']} in trade"
                )

    if nft_in_sent and not nft_in_received:
        order_side, buyer, seller = "listing", caller, signer
    elif nft_in_received and not nft_in_sent:
        order_side, buyer, seller = "bid", signer, caller
    else:
        order_side, buyer, seller = "unknown", None, None

    return {
        "trade_id": trade_id,
        "caller": caller,
        "signer": signer,
        "order_side": order_side,
        "buyer": buyer,
        "seller": seller,
        "nft_contract_addresses": nft_contracts,
        "nft_token_ids": nft_token_ids,
        "nft_asset_types": nft_asset_types,
        "nft_beneficiaries": nft_beneficiaries,
        "payment_currencies": pay_currencies,
        "payment_amounts": pay_amounts,
        "payment_asset_types": pay_asset_types,
        "payment_beneficiaries": pay_beneficiaries,
    }
