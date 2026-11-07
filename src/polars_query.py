"""Native Polars WordCount matching the Worker's word definition."""
import polars as pl


def wordcount(source):
    # Rust regex \w additionally includes combining marks/connectors. Python's
    # Unicode \w is alphanumeric (L/N categories) plus underscore.
    return (pl.scan_lines(source)
            .select(pl.col('line').str.to_lowercase()
                    .str.extract_all(r'[\p{L}\p{N}_]+').alias('word'))
            .explode('word', empty_as_null=True)
            .filter(pl.col('word').is_not_null())
            .group_by('word')
            .agg(pl.len().cast(pl.UInt64).alias('count')))

