#[allow(unused)]
mod pb;

use pb::sf::solana::r#type::v1::{Block, ConfirmedTransaction};

#[substreams::handlers::map]
fn map_my_data(block: Block) -> pb::mydata::v1::MyData {
    let mut output = pb::mydata::v1::MyData::default();

    for confirmed_tx in block.transactions.iter() {
        extract_transfers(confirmed_tx, block.slot, &mut output);
    }

    output
}

fn extract_transfers(
    confirmed_tx: &ConfirmedTransaction,
    block_num: u64,
    output: &mut pb::mydata::v1::MyData,
) {
    let meta = match &confirmed_tx.meta {
        Some(meta) => meta,
        None => return,
    };

    if meta.err.is_some() {
        return;
    }

    let tx = match &confirmed_tx.transaction {
        Some(tx) => tx,
        None => return,
    };

    let message = match &tx.message {
        Some(msg) => msg,
        None => return,
    };

    let account_keys = &message.account_keys;
    let pre_balances = &meta.pre_balances;
    let post_balances = &meta.post_balances;

    if pre_balances.len() != post_balances.len() {
        return;
    }

    let mut sender_index: Option<usize> = None;
    let mut receiver_index: Option<usize> = None;
    let mut amount: u64 = 0;

    for i in 0..pre_balances.len() {
        let pre = pre_balances[i];
        let post = post_balances[i];

        if post < pre {
            sender_index = Some(i);
            amount = pre - post;
        }

        if post > pre {
            receiver_index = Some(i);
        }
    }

    if let (Some(from_i), Some(to_i)) = (sender_index, receiver_index) {
        if let (Some(from_bytes), Some(to_bytes)) = (
            account_keys.get(from_i),
            account_keys.get(to_i),
        ) {
            let from = bs58::encode(from_bytes).into_string();
            let to = bs58::encode(to_bytes).into_string();

            let tx_id = match tx.signatures.get(0) {
                Some(sig) => bs58::encode(sig).into_string(),
                None => String::from(""),
            };

            output.transfers.push(pb::mydata::v1::Transfer {
                from,
                to,
                amount,
                block_num,
                tx_id,
            });
        }
    }
}