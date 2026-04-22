use bitcoin::Transaction;
use bitcoin::consensus::deserialize;

fn main() {
    let hex_tx = std::env::args().nth(1).expect("hex tx");
    let bytes = hex::decode(hex_tx).unwrap();
    let tx: Transaction = deserialize(&bytes).unwrap();
    let envs = dmt_indexer::inscription::envelope::parse_envelopes(&tx);
    println!("tx has {} inputs, parser extracted {} envelopes", tx.input.len(), envs.len());
    for e in &envs {
        println!("  env: input_index={} content_type={:?} content_enc={:?} content_len={}",
            e.input_index, e.content_type, e.content_encoding, e.content.len());
        let body = e.decoded_content();
        let s = String::from_utf8_lossy(&body);
        let preview = if s.len() > 120 { format!("{}...", &s[..120]) } else { s.into_owned() };
        println!("    body: {}", preview);
    }
}
