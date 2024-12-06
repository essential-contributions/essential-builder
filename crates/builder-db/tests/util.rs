#![allow(dead_code)]

use essential_hash::content_addr;
use essential_node_types::{Block, BlockHeader};
use essential_types::{
    contract::Contract,
    predicate::{Edge, Node, Reads},
    solution::{Solution, SolutionSet},
    ContentAddress, Predicate, PredicateAddress, Program, Word,
};
use std::time::Duration;

pub fn test_blocks(n: Word) -> Vec<Block> {
    (0..n)
        .map(|i| test_block(i, Duration::from_secs(i as _)))
        .collect()
}

pub fn test_block(number: Word, timestamp: Duration) -> Block {
    let seed = number * 79;
    Block {
        header: BlockHeader { number, timestamp },
        solution_sets: (0..3).map(|i| test_solution_set(seed * (1 + i))).collect(),
    }
}

pub fn test_solution_set(seed: Word) -> SolutionSet {
    SolutionSet {
        solutions: vec![test_solution(seed)],
    }
}

pub fn test_solution(seed: Word) -> Solution {
    let contract = test_contract(seed);
    let predicate = essential_hash::content_addr(&contract.predicates[0]);
    let contract = essential_hash::content_addr(&contract);
    Solution {
        predicate_to_solve: PredicateAddress {
            contract,
            predicate,
        },
        predicate_data: vec![],
        state_mutations: vec![],
    }
}

pub fn test_pred_addr() -> PredicateAddress {
    PredicateAddress {
        contract: [0xAA; 32].into(),
        predicate: [0xAA; 32].into(),
    }
}

pub fn test_contract(seed: Word) -> Contract {
    let n = (1 + seed % 2) as usize;
    Contract {
        // Make sure each predicate is unique, or contract will have a different
        // number of entries after insertion when multiple predicates have same CA.
        predicates: (0..n)
            .map(|ix| test_predicate(seed * (1 + ix as i64)))
            .collect(),
        salt: essential_types::convert::u8_32_from_word_4([seed; 4]),
    }
}

pub fn test_predicate(seed: Word) -> Predicate {
    use essential_check::vm::asm::{self, short::*};

    let a = Program(asm::to_bytes([PUSH(1), PUSH(2), PUSH(3), HLT]).collect());
    let b = Program(asm::to_bytes([PUSH(seed), HLT]).collect());
    let c = Program(
        asm::to_bytes([
            // Stack should already have `[1, 2, 3, seed]`.
            PUSH(1),
            PUSH(2),
            PUSH(3),
            PUSH(seed),
            // a `len` for `EqRange`.
            PUSH(4), // EqRange len
            EQRA,
            HLT,
        ])
        .collect(),
    );

    let a_ca = content_addr(&a);
    let b_ca = content_addr(&b);
    let c_ca = content_addr(&c);

    let node = |program_address, edge_start| Node {
        program_address,
        edge_start,
        reads: Reads::Pre, // unused for this test.
    };
    let nodes = vec![
        node(a_ca.clone(), 0),
        node(b_ca.clone(), 1),
        node(c_ca.clone(), Edge::MAX),
    ];
    let edges = vec![2, 2];
    Predicate { nodes, edges }
}

pub fn get_block_address(i: Word) -> ContentAddress {
    content_addr(&Block {
        header: BlockHeader {
            number: i,
            timestamp: Default::default(),
        },
        solution_sets: Default::default(),
    })
}
