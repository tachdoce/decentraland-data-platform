import json
from pathlib import Path

import pytest
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode

from decode.ethereum_marketplace_trades_parser import parse_traded
from decode.ethereum_marketplace_trades_query import TRADED_TOPIC

FIXTURE = Path(__file__).parent / "fixtures" / "marketplace_trades_2025.json"

CHECKS_T = (
    "(uint256,uint256,uint256,bytes32,uint256,uint256,uint256,"
    "address[],(address,bytes4,uint256,bool)[])"
)
ASSET_T = "(uint256,address,uint256,address,bytes)"
TRADE_T = f"(address,bytes,{CHECKS_T},{ASSET_T}[],{ASSET_T}[])"

_EMPTY_CHECKS = (0, 0, 0, b"\x00" * 32, 0, 0, 0, [], [])


def _topics(caller: str, trade_id: str) -> list[str]:
    return [
        TRADED_TOPIC,
        "0x" + "0" * 24 + caller.removeprefix("0x"),
        trade_id,
    ]


def _encode_trade(signer, sent, received) -> str:
    return "0x" + abi_encode(
        [TRADE_T], [(signer, b"", _EMPTY_CHECKS, sent, received)]
    ).hex()


def test_parser_matches_eth_abi_on_real_logs():
    records = json.loads(FIXTURE.read_text())
    assert len(records) == 4
    saw_extra = False
    for record in records:
        parsed = parse_traded(record["topics"], record["data"])
        (trade,) = abi_decode([TRADE_T], bytes.fromhex(record["data"][2:]))
        signer, _sig, _checks, sent, received = trade
        saw_extra = saw_extra or any(a[4] for a in list(sent) + list(received))

        assert parsed["caller"] == "0x" + record["topics"][1][26:].lower()
        assert parsed["trade_id"] == record["topics"][2]
        assert parsed["signer"] == signer.lower()
        ref_nfts = [a for a in list(sent) + list(received) if a[0] in (3, 4)]
        ref_pays = [a for a in list(sent) + list(received) if a[0] in (1, 2)]
        assert parsed["nft_contract_addresses"] == [a[1].lower() for a in ref_nfts]
        assert parsed["nft_token_ids"] == [str(a[2]) for a in ref_nfts]
        assert parsed["nft_asset_types"] == [a[0] for a in ref_nfts]
        assert parsed["nft_beneficiaries"] == [a[3].lower() for a in ref_nfts]
        assert parsed["payment_currencies"] == [a[1].lower() for a in ref_pays]
        assert parsed["payment_amounts"] == [a[2] for a in ref_pays]
        assert parsed["payment_asset_types"] == [a[0] for a in ref_pays]
        assert parsed["payment_beneficiaries"] == [a[3].lower() for a in ref_pays]
        # all sampled logs are listings: NFTs sit in sent
        assert parsed["order_side"] == "listing"
        assert parsed["seller"] == parsed["signer"]
        assert parsed["buyer"] == parsed["caller"]
    # the fixture set must exercise the non-empty `extra` layout
    assert saw_extra


def test_parser_synthetic_bid_nft_in_received():
    # bid: signer offers MANA (sent), receives the NFT
    signer = "0x" + "a1" * 20
    caller = "0x" + "b2" * 20
    mana = "0x0f5d2fb29fb7d3cfee444a200298f468908cc942"
    land = "0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d"
    data = _encode_trade(
        signer,
        sent=[(1, mana, 5 * 10**18, "0x" + "c3" * 20, b"")],
        received=[(3, land, 42, signer, b"")],
    )
    parsed = parse_traded(_topics(caller, "0x" + "11" * 32), data)
    assert parsed["order_side"] == "bid"
    assert parsed["buyer"] == signer
    assert parsed["seller"] == caller
    assert parsed["nft_token_ids"] == ["42"]
    assert parsed["payment_amounts"] == [5 * 10**18]


def test_parser_usd_pegged_and_collection_item_types():
    signer = "0x" + "a1" * 20
    mana = "0x0f5d2fb29fb7d3cfee444a200298f468908cc942"
    coll = "0x" + "d4" * 20
    data = _encode_trade(
        signer,
        sent=[(4, coll, 7, "0x" + "b2" * 20, b"")],
        received=[(2, mana, 10 * 10**18, signer, b"")],
    )
    parsed = parse_traded(_topics("0x" + "b2" * 20, "0x" + "22" * 32), data)
    assert parsed["nft_asset_types"] == [4]
    assert parsed["payment_asset_types"] == [2]
    assert parsed["order_side"] == "listing"


def test_parser_unknown_side_nfts_both_sides():
    signer = "0x" + "a1" * 20
    land = "0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d"
    data = _encode_trade(
        signer,
        sent=[(3, land, 1, signer, b"")],
        received=[(3, land, 2, signer, b"")],
    )
    parsed = parse_traded(_topics("0x" + "b2" * 20, "0x" + "33" * 32), data)
    assert parsed["order_side"] == "unknown"
    assert parsed["buyer"] is None and parsed["seller"] is None


def test_parser_rejects_wrong_topic_and_bad_asset_type():
    with pytest.raises(ValueError):
        parse_traded(["0x" + "ff" * 32, "0x" + "0" * 64, "0x" + "0" * 64], "0x")
    signer = "0x" + "a1" * 20
    data = _encode_trade(
        signer, sent=[(9, "0x" + "d4" * 20, 1, signer, b"")], received=[]
    )
    with pytest.raises(ValueError):
        parse_traded(_topics("0x" + "b2" * 20, "0x" + "44" * 32), data)
