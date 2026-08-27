import json
from pathlib import Path

import pytest
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode

from decode.seaport_parser import (
    ORDER_FULFILLED_TOPIC,
    ZERO_ADDRESS,
    parse_order_fulfilled,
)

FIXTURE = Path(__file__).parent / "fixtures" / "seaport_order_fulfilled_2026-08-17.json"
LISTING_FIXTURE = Path(__file__).parent / "fixtures" / "seaport_listing_2026-08-17.json"

# abi types of the non-indexed OrderFulfilled params:
# (orderHash, recipient, SpentItem[] offer, ReceivedItem[] consideration)
ABI_TYPES = [
    "bytes32",
    "address",
    "(uint8,address,uint256,uint256)[]",
    "(uint8,address,uint256,uint256,address)[]",
]

WETH = "0x" + "ee" * 20
LAND = "0x" + "ab" * 20
ALICE = "0x" + "aa" * 20
BOB = "0x" + "bb" * 20
FEE_WALLET = "0x" + "fe" * 20


def _load_fixture():
    return json.loads(FIXTURE.read_text())


def _encode_log(order_hash, recipient, offer, consideration, offerer):
    """Build (topics, data) exactly as Seaport emits them."""
    data = "0x" + abi_encode(
        ABI_TYPES, [order_hash, recipient, offer, consideration]
    ).hex()
    topics = [
        ORDER_FULFILLED_TOPIC,
        "0x" + "00" * 12 + offerer[2:],  # indexed offerer, left-padded
        "0x" + "00" * 32,  # indexed zone (unused by the parser)
    ]
    return topics, data


def test_real_logs_match_eth_abi():
    for record in _load_fixture():
        parsed = parse_order_fulfilled(record["topics"], record["data"])
        order_hash, recipient, offer, consideration = abi_decode(
            ABI_TYPES, bytes.fromhex(record["data"][2:])
        )
        assert parsed["order_hash"] == "0x" + order_hash.hex()
        assert parsed["recipient"] == recipient.lower()
        n_items = len(offer) + len(consideration)
        n_parsed = len(parsed["nft_contract_addresses"]) + len(
            parsed["payment_amounts"]
        )
        assert n_parsed == n_items  # every item routed exactly once
        # parallel arrays stay parallel
        assert (
            len(parsed["nft_contract_addresses"])
            == len(parsed["nft_token_ids"])
            == len(parsed["nft_quantities"])
            == len(parsed["nft_froms"])
            == len(parsed["nft_tos"])
        )
        assert (
            len(parsed["payment_currencies"])
            == len(parsed["payment_amounts"])
            == len(parsed["payment_recipients"])
        )


def test_real_swap_legs_classify_as_non_sales():
    # The fixture tx is a matchOrders NFT swap: LAND #12302 + 5.2 WETH
    # traded for three LANDs. Neither leg is a sale.
    by_index = {r["log_index"]: r for r in _load_fixture()}
    swap = parse_order_fulfilled(by_index[497]["topics"], by_index[497]["data"])
    counterleg = parse_order_fulfilled(by_index[498]["topics"], by_index[498]["data"])
    # leg 497: NFTs on both sides -> unknown, no buyer/seller guessed
    assert swap["order_side"] == "unknown"
    assert swap["buyer"] is None and swap["seller"] is None
    assert len(swap["nft_contract_addresses"]) == 5  # 1 offered + 4 in consideration
    assert len(swap["payment_amounts"]) == 2  # WETH appears on both sides
    # leg 498: 3 NFTs offered, empty consideration -> listing shape, zero payments
    assert counterleg["order_side"] == "listing"
    assert len(counterleg["nft_contract_addresses"]) == 3
    assert counterleg["payment_amounts"] == []


def test_real_plain_listing():
    record = json.loads(LISTING_FIXTURE.read_text())
    parsed = parse_order_fulfilled(record["topics"], record["data"])
    assert parsed["order_side"] == "listing"
    assert parsed["buyer"] == parsed["recipient"]
    assert parsed["seller"] == parsed["offerer"]
    assert parsed["nft_contract_addresses"] == [
        "0x8a1bbef259b00ced668a8c69e50d92619c672176"
    ]
    assert parsed["nft_token_ids"] == ["5046"]
    assert parsed["nft_froms"] == [parsed["offerer"]]
    assert parsed["nft_tos"] == [parsed["recipient"]]
    assert parsed["payment_currencies"] == [
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"  # WETH
    ]
    assert parsed["payment_amounts"] == [172260000000000000]


def test_synthetic_bid_roundtrip():
    # Bob (offerer) bids 1 WETH for Alice's LAND; Alice fulfills.
    topics, data = _encode_log(
        order_hash=b"\x01" * 32,
        recipient=ALICE,  # fulfiller
        offer=[(1, WETH, 0, 10**18)],  # ERC-20 -> payment side
        consideration=[
            (2, LAND, (10 << 128) | 20, 1, BOB),  # ERC-721 to Bob
            (1, WETH, 5 * 10**16, 5 * 10**16, FEE_WALLET),
        ],
        offerer=BOB,
    )
    parsed = parse_order_fulfilled(topics, data)
    assert parsed["order_side"] == "bid"
    assert parsed["buyer"] == BOB and parsed["seller"] == ALICE
    assert parsed["nft_contract_addresses"] == [LAND]
    assert parsed["nft_token_ids"] == [str((10 << 128) | 20)]  # decimal string
    assert parsed["nft_quantities"] == [1]
    assert parsed["nft_froms"] == [ALICE]  # event recipient fulfilled
    assert parsed["nft_tos"] == [BOB]  # item-level recipient
    assert parsed["payment_currencies"] == [WETH, WETH]
    assert parsed["payment_amounts"] == [10**18, 5 * 10**16]
    # offer-side payment goes to the fulfiller; consideration fee to its recipient
    assert parsed["payment_recipients"] == [ALICE, FEE_WALLET]


def test_zero_recipient_yields_nulls_not_guesses():
    topics, data = _encode_log(
        order_hash=b"\x02" * 32,
        recipient=ZERO_ADDRESS,  # matchOrders-style fulfillment
        offer=[(2, LAND, 7, 1)],
        consideration=[(0, ZERO_ADDRESS, 0, 10**18, ALICE)],
        offerer=ALICE,
    )
    parsed = parse_order_fulfilled(topics, data)
    assert parsed["order_side"] == "listing"
    assert parsed["seller"] == ALICE
    assert parsed["buyer"] is None
    assert parsed["nft_tos"] == [None]
    assert parsed["nft_froms"] == [ALICE]


def test_native_eth_payment_uses_zero_address_currency():
    topics, data = _encode_log(
        order_hash=b"\x03" * 32,
        recipient=BOB,
        offer=[(2, LAND, 7, 1)],
        consideration=[(0, ZERO_ADDRESS, 0, 10**18, ALICE)],
        offerer=ALICE,
    )
    parsed = parse_order_fulfilled(topics, data)
    assert parsed["payment_currencies"] == [ZERO_ADDRESS]


def test_nfts_on_both_sides_is_unknown():
    topics, data = _encode_log(
        order_hash=b"\x04" * 32,
        recipient=BOB,
        offer=[(2, LAND, 1, 1)],
        consideration=[(2, LAND, 2, 1, ALICE)],
        offerer=ALICE,
    )
    parsed = parse_order_fulfilled(topics, data)
    assert parsed["order_side"] == "unknown"
    assert parsed["buyer"] is None and parsed["seller"] is None


def test_wrong_topic_raises():
    with pytest.raises(ValueError, match="topic"):
        parse_order_fulfilled(["0x" + "00" * 32, "0x" + "00" * 32], "0x")


def test_truncated_data_raises():
    record = _load_fixture()[0]
    with pytest.raises(ValueError, match="truncated"):
        parse_order_fulfilled(record["topics"], record["data"][:200])
