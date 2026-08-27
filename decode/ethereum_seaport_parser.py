"""Pure decoder for Seaport OrderFulfilled logs.

No eth_abi at runtime: the event data has a regular ABI layout that a
word-by-word reader covers — head of 4 static words (orderHash,
recipient, offset of offer[], offset of consideration[]) followed by two
arrays of static structs (SpentItem = 4 words, ReceivedItem = 5). The
manual parse is validated against eth_abi in tests.

The parser mirrors the event honestly: no per-payment payer exists in
the log, so none is produced; buyer/seller/order_side are the only
derived fields, and they go NULL rather than guessed when the event's
recipient is the zero address.
"""

ORDER_FULFILLED_TOPIC = (
    "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"
)
ZERO_ADDRESS = "0x" + "0" * 40

_ITEM_NFT = {2, 3}  # ERC-721, ERC-1155
_ITEM_PAYMENT = {0, 1}  # native ETH, ERC-20


def _word(data_hex: str, index: int) -> str:
    word = data_hex[index * 64 : (index + 1) * 64]
    if len(word) < 64:
        raise ValueError(f"data truncated at word {index}")
    return word


def _address(word: str) -> str:
    return ("0x" + word[-40:]).lower()


def _read_items(data_hex: str, offset_word: int, words_per_item: int) -> list[dict]:
    count = int(_word(data_hex, offset_word), 16)
    items = []
    for i in range(count):
        base = offset_word + 1 + i * words_per_item
        item = {
            "item_type": int(_word(data_hex, base), 16),
            "token": _address(_word(data_hex, base + 1)),
            "identifier": int(_word(data_hex, base + 2), 16),
            "amount": int(_word(data_hex, base + 3), 16),
        }
        if words_per_item == 5:
            item["recipient"] = _address(_word(data_hex, base + 4))
        items.append(item)
    return items


def parse_order_fulfilled(topics: list[str], data: str) -> dict:
    if not topics or topics[0] != ORDER_FULFILLED_TOPIC:
        raise ValueError(f"not an OrderFulfilled topic: {topics[:1]}")
    offerer = _address(topics[1])

    data_hex = data.removeprefix("0x")
    order_hash = "0x" + _word(data_hex, 0)
    recipient = _address(_word(data_hex, 1))
    # offsets are byte positions from the start of data
    offer_at = int(_word(data_hex, 2), 16) // 32
    consideration_at = int(_word(data_hex, 3), 16) // 32
    offer = _read_items(data_hex, offer_at, 4)
    consideration = _read_items(data_hex, consideration_at, 5)

    recipient_or_none = None if recipient == ZERO_ADDRESS else recipient

    nft_contracts: list[str] = []
    nft_token_ids: list[str] = []
    nft_quantities: list[int] = []
    nft_froms: list[str | None] = []
    nft_tos: list[str | None] = []
    pay_currencies: list[str] = []
    pay_amounts: list[int] = []
    pay_recipients: list[str | None] = []
    nft_in_offer = nft_in_consideration = False

    for side, items in (("offer", offer), ("consideration", consideration)):
        for item in items:
            if item["item_type"] in _ITEM_NFT:
                if side == "offer":
                    nft_in_offer = True
                    nft_from, nft_to = offerer, recipient_or_none
                else:
                    nft_in_consideration = True
                    nft_from, nft_to = recipient_or_none, item["recipient"]
                nft_contracts.append(item["token"])
                nft_token_ids.append(str(item["identifier"]))
                nft_quantities.append(item["amount"])
                nft_froms.append(nft_from)
                nft_tos.append(nft_to)
            elif item["item_type"] in _ITEM_PAYMENT:
                pay_currencies.append(item["token"])
                pay_amounts.append(item["amount"])
                # offer-side payments (accepted bids) flow to the fulfiller
                pay_recipients.append(
                    item.get("recipient")
                    if side == "consideration"
                    else recipient_or_none
                )
            else:
                raise ValueError(
                    f"unexpected itemType {item['item_type']} in fulfilled order"
                )

    if nft_in_offer and not nft_in_consideration:
        order_side, buyer, seller = "listing", recipient_or_none, offerer
    elif nft_in_consideration and not nft_in_offer:
        order_side, buyer, seller = "bid", offerer, recipient_or_none
    else:
        order_side, buyer, seller = "unknown", None, None

    return {
        "order_hash": order_hash,
        "offerer": offerer,
        "recipient": recipient,
        "order_side": order_side,
        "buyer": buyer,
        "seller": seller,
        "nft_contract_addresses": nft_contracts,
        "nft_token_ids": nft_token_ids,
        "nft_quantities": nft_quantities,
        "nft_froms": nft_froms,
        "nft_tos": nft_tos,
        "payment_currencies": pay_currencies,
        "payment_amounts": pay_amounts,
        "payment_recipients": pay_recipients,
    }
