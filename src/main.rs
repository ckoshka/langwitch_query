use std::collections::HashMap;
// THIS WILL NOT WORK UNTIL DICT IS ADDED BACK TO LW_ENCODE and fnvhash
use nohash_hasher::IntSet;
use rayon::prelude::*;
use term_macros::*;

type Contexts = Vec<IntSet<u32>>;

fn main() {
    tool! {
        args:
            - ctxs_filename: String;
            - sentences_file: String;
        ;
        body: || {
            let sentences_mmap = mmap!(sentences_file);
            let mut nls: Vec<_> = vec![];
            let mut ctxs: Contexts = vec![];
            let mut dict: HashMap<String, u32> = HashMap::new();
            rayon::scope(|s| {
                s.spawn(|_| {
                    nls = sentences_mmap.par_iter()
                        .enumerate()
                        .filter(|(_, b)| **b == b'\n')
                        .map(|(i, _)| i)
                        .collect();
                });

                s.spawn(|_| {
                    let ctxs_mmap = mmap!(ctxs_filename);
                    ctxs = rmp_serde::from_slice(&ctxs_mmap[..]).unwrap();
                });

            });

            let get_sentence = |line_number: usize| {
                let line_number = line_number - 1;
                let start = nls[line_number] + 1;
                let end = nls[line_number+1];
                &sentences_mmap[start..end]
            };

            //let reversed_dict: HashMap<&u32, &String> = dict.iter().map(|(k, j)| (j, k)).collect();
            
            let mut stdout = std::io::stdout().lock();

            //eprintln!("Ready");
            stdout.flush().unwrap();

            readin!(_wtr, |line: &[u8]| {
                let request = std::str::from_utf8(line).unwrap();
                // first part will consist of knowns, encoded as u32 values,
                // then the second part will consist of the one focus word.
                // a sentence must contain the focus word, and the rest must only consist of known words.

                let mut parts = request.split("|||");
                let focus_word = parts.next()
                    .map(|w| w.trim())
                    .map(|w| dict.get(w))
                    .unwrap();

                if focus_word.is_none() {
                    stdout.write_all(b"\n").unwrap();
                    return
                }

                let focus_word = *focus_word.unwrap();

                let mut known_words = parts.next()
                    .map(|ws| ws.split("||")
                        .map(|w| 
                            dict.get(w)
                        )
                        .filter(|w| w.is_some())
                        .map(|w| *w.unwrap())
                        .collect::<IntSet<_>>()
                    ).unwrap();

                //println!("Known words: {:#?}", known_words);
                known_words.insert(focus_word);
                
                let valid_ids = ctxs
                    .par_iter()
                    .enumerate()
                    .filter(|(_, ctx)| {
                        ctx.contains(&focus_word) && ctx.difference(&known_words).next().is_none()
                    })
                    .map(|(i, _)| (i, get_sentence(i)))
                    .collect::<Vec<_>>();

                valid_ids.iter().for_each(|(i, sentence)| {
                    stdout.write_all(i.to_string().as_bytes()).unwrap();
                    stdout.write_all(b"|").unwrap();
                    stdout.write_all(sentence).unwrap();
                    stdout.write_all(b"\n").unwrap();
                });

                stdout.write_all(b"\n").unwrap();

                stdout.flush().unwrap();

            });
        }
    }
}
