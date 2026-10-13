use bumpalo::Bump;
use criterion::{Criterion, criterion_group, criterion_main};
use std::fmt::Write;

fn gen_str_bumpalo<'a>(
    table: &str,
    keys: &[String],
    arena: &'a Bump,
) -> bumpalo::collections::String<'a> {
    let mut stmt_str =
        bumpalo::collections::String::with_capacity_in(19 + table.len() + keys.len() * 20, arena);
    stmt_str.push_str("DELETE FROM ");
    stmt_str.push_str(table);
    stmt_str.push_str(" WHERE ");

    for (i, key) in keys.iter().enumerate() {
        if i < keys.len() - 1 {
            write!(&mut stmt_str, "{} = ${} AND ", key, i + 1).expect("");
        } else {
            write!(&mut stmt_str, "{} = ${}", key, i + 1).expect("");
        }
    }

    stmt_str
}

fn gen_str(table: &str, keys: &[String]) -> String {
    let mut stmt_str = String::with_capacity(19 + table.len() + keys.len() * 20);
    stmt_str.push_str("DELETE FROM ");
    stmt_str.push_str(table);
    stmt_str.push_str(" WHERE ");

    for (i, key) in keys.iter().enumerate() {
        if i < keys.len() - 1 {
            write!(&mut stmt_str, "{} = ${} AND ", key, i + 1).expect("");
        } else {
            write!(&mut stmt_str, "{} = ${}", key, i + 1).expect("");
        }
    }

    stmt_str
}

fn string_len(c: &mut Criterion) {
    let mut arena = Bump::with_capacity(4096);

    let keys = [
        "asjdijsd".to_string(),
        "isjdoisjdoij".to_string(),
        "sdijdoisjdsoid".to_string(),
    ];

    let mut group = c.benchmark_group("statement_creation");
    group.bench_function("raw_str", |b| b.iter(|| gen_str("users_three", &keys)));
    group.bench_function("bumpalo", |b| {
        b.iter(|| gen_str_bumpalo("users_three", &keys, &arena));
        arena.reset();
    });
    group.finish();
}

//criterion_group!(benches, bench);
criterion_group!(benches, string_len);
criterion_main!(benches);
